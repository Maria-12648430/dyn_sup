-module(dyn_sup).
-behavior(gen_server).

-export([start_link/2, start_link/3]).
-export([start_child/2, terminate_child/2]).
-export([which_children/1, count_children/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-callback init(Args :: term()) -> term().

-record(sup_flags, {intensity, period}).
-record(child_spec, {id, module, function, args, restart, shutdown, type, modules}).
-record(state, {module, args, flags, spec, children, restarting, terminating}).

-spec start_link(_, _) -> _.
start_link(Mod, Args) ->
        gen_server:start_link(?MODULE, {undefined, Mod, Args}, []).

-spec start_link(_, _, _) -> _.
start_link(SupName, Mod, Args) ->
        gen_server:start_link(SupName, ?MODULE, {SupName, Mod, Args}, []).

-spec start_child(_, _) -> _.
start_child(SupRef, Args) when is_list(Args) ->
        gen_server:call(SupRef, {start_child, Args}, infinity).

-spec terminate_child(_, _) -> _.
terminate_child(SupRef, Pid) when is_pid(Pid) ->
        gen_server:call(SupRef, {terminate_child, Pid}, infinity).

-spec which_children(_) -> _.
which_children(SupRef) ->
        gen_server:call(SupRef, which_children, infinity).

-spec count_children(_) -> _.
count_children(SupRef) ->
        gen_server:call(SupRef, count_children, infinity).

-spec init(_) -> _.
init({_SupName, Mod, Args}) ->
        process_flag(trap_exit, true),
        case Mod:init(Args) of
                {ok, {Flags, Spec}} ->
                        case check_init(Flags, Spec) of
                                {ok, F, S} ->
                                        {ok, #state{module=Mod, args=Args, flags=F, spec=S, children=#{}, restarting=#{}, terminating=#{}}};
                                {error, Error} ->
                                        {stop, {supervisor_data, Error}}
                        end;
                ignore ->
                        ignore;
                Error ->
                        {stop, {bad_return, {Mod, init, Error}}}
        end.

-spec handle_call(_, _, _) -> _.
handle_call(which_children, From, State=#state{spec=#child_spec{type=Type, modules=Modules}, children=Children, terminating=Terminating, restarting=Restarting}) ->
        spawn(fun() ->
                  ChildList=[{undefined, Pid, Type, Modules} || Pid <- maps:keys(Children)],
                  TerminatingList=[{undefined, Pid, Type, Modules} || Pid <- maps:keys(Terminating)],
                  RestartingList=[{undefined, restarting, Type, Modules} || _ <- maps:keys(Restarting)],
                  gen_server:reply(From, ChildList++TerminatingList++RestartingList, State)
              end),
        {noreply, State};
handle_call(count_children, From, State=#state{spec=#child_spec{type=Type}, children=Children, terminating=Terminating, restarting=Restarting}) ->
        spawn(fun() ->
                  Active = maps:size(Children) + maps:size(Terminating),
                  All = Active + maps:size(Restarting),
                  Reply = case Type of
                              worker -> [{specs, 1}, {active, Active}, {supervisors, 0}, {workers, All}];
                              supervisor -> [{specs, 1}, {active, Active}, {supervisors, All}, {workers, 0}]
                          end,
                  gen_server:reply(From, Reply)
              end),
        {noreply, State};
handle_call({start_child, Args}, _From, State=#state{children=Children, spec=#child_spec{module=M, function=F, args=A}}) ->
        case start_child(M, F, A, Args) of
                {ok, Pid} ->
                        Ref = monitor(process, Pid, [{tag, 'CHILD-DOWN'}]),
                        {reply, {ok, Pid}, State#state{children=Children#{Pid => {Ref, Args}}}};
                {ok, Pid, _Extra} ->
                        Ref = monitor(process, Pid, [{tag, 'CHILD-DOWN'}]),
                        {reply, {ok, Pid}, State#state{children=Children#{Pid => {Ref, Args}}}};
                ignore ->
                        {reply, {ok, undefined}, State};
                Other ->
                        {reply, Other, State}
        end;
handle_call({terminate_child, Pid}, From, State=#state{children=Children, terminating=Terminating, spec=#child_spec{shutdown=Shutdown}}) when is_map_key(Pid, Children) ->
        {{Ref, _}, Children1} = maps:take(Pid, Children),
        Timer = if
                    Shutdown=:=brutal_kill ->
                            exit(Pid, kill),
                            undefined;
                    Shutdown=:=infinity ->
                            exit(Pid, shutdown),
                            undefined;
                    true ->
                            exit(Pid, shutdown),
                            erlang:start_timer(Shutdown, self(), {terminate_timeout, Pid})
                end,
        {noreply, State#state{children=Children1, terminating=Terminating#{Pid => {Ref, Timer, [From]}}}};
handle_call({terminate_child, Pid}, From, State=#state{terminating=Terminating}) when is_map_key(Pid, Terminating) ->
        #{Pid := {Ref, Timer, ReplyTo}} = Terminating,
        {noreply, State#state{terminating=Terminating#{Pid => {Ref, Timer, [From|ReplyTo]}}}};
handle_call(_Msg, _From, State) ->
        {noreply, State}.

-spec handle_cast(_, _) -> _.
handle_cast({try_restart, Ref}, State=#state{children=Children, spec=#child_spec{module=M, function=F, args=A}, restarting=Restarting}) when is_map_key(Ref, Restarting) ->
        {Args, Restarting1}=maps:take(Ref, Restarting),
        case start_child(M, F, A, Args) of
                {ok, Pid} ->
                        Mon = monitor(process, Pid, [{tag, 'CHILD-DOWN'}]),
                        {noreply, State#state{children=Children#{Pid => {Mon, Args}}, restarting=Restarting1}};
                {ok, Pid, _Extra} ->
                        Mon = monitor(process, Pid, [{tag, 'CHILD-DOWN'}]),
                        {noreply, State#state{children=Children#{Pid => {Mon, Args}}, restarting=Restarting1}};
                ignore ->
                        {noreply, State#state{restarting=Restarting1}};
                {error, _} ->
                        gen_server:cast(self(), {try_restart, Ref}),
                        {noreply, State}
        end;
handle_cast(_Msg, State) ->
        {noreply, State}.

-spec handle_info(_, _) -> _.
handle_info({'CHILD-DOWN', Mon, process, Pid, Reason}, State=#state{terminating=Terminating}) when is_map_key(Pid, Terminating) ->
        case maps:take(Pid, Terminating) of
                {{Mon, Timer, ReplyTo}, Terminating1} ->
                        maybe_cancel_timer(Timer),
                        Reply = case unlink_flush(Pid, Reason) of
                                    killed -> ok;
                                    normal -> ok;
                                    shutdown -> ok;
                                    {shutdown, _} -> ok;
                                    Reason1 -> {error, Reason1}
                                end,
                        reply_all(ReplyTo, Reply),
                        {noreply, State#state{terminating=Terminating1}};
                _ ->
                        {noreply, State}
        end;
handle_info({timeout, Timer, {terminate_timeout, Pid}}, State=#state{terminating=Terminating}) when is_map_key(Pid, Terminating) ->
        case Terminating of
                #{Pid := {Mon, Timer, ReplyTo}} ->
                        exit(Pid, kill),
                        {noreply, State#state{terminating=Terminating#{Pid => {Mon, undefined, ReplyTo}}}};
                _ ->
                        {noreply, State}
        end;
handle_info({'CHILD-DOWN', Mon, process, Pid, Reason}, State=#state{children=Children, spec=#child_spec{restart=temporary}}) when is_map_key(Pid, Children) ->
        case maps:take(Pid, Children) of
                {{Mon, _}, Children1} ->
                        unlink_flush(Pid, Reason),
                        {noreply, State#state{children=Children1}};
                _ ->
                        {noreply, State}
        end;
handle_info({'CHILD-DOWN', Mon, process, Pid, Reason}, State=#state{children=Children, spec=#child_spec{restart=transient, module=M, function=F, args=A}, restarting=Restarting}) when is_map_key(Pid, Children) ->
        case maps:take(Pid, Children) of
                {{Mon, Args}, Children1} ->
                        DoRestart = case unlink_flush(Pid, Reason) of
                                        normal -> false;
                                        shutdown -> false;
                                        {shutdown, _} -> false;
                                        _ -> true
                                    end,
                        case DoRestart andalso start_child(M, F, A, Args) of
                                false ->
                                        {noreply, State#state{children=Children1}};
                                ignore ->
                                        {noreply, State#state{children=Children1}};
                                {ok, NewPid} ->
                                        NewMon = monitor(process, NewPid, [{tag, 'CHILD-DOWN'}]),
                                        {noreply, State#state{children=Children1#{NewPid => {NewMon, Args}}}};
                                {ok, NewPid, _Extra} ->
                                        NewMon = monitor(process, NewPid, [{tag, 'CHILD-DOWN'}]),
                                        {noreply, State#state{children=Children1#{NewPid => {NewMon, Args}}}};
                                {error, _} ->
                                        Ref = make_ref(),
                                        gen_server:cast(self(), {try_restart, Ref}),
                                        {noreply, State#state{children=Children1, restarting=Restarting#{Ref => Args}}}
                end
        end;
handle_info(_Msg, State) ->
        {noreply, State}.

-spec terminate(_, _) -> _.
terminate(_Reason, #state{children=Children, terminating=Terminating}) when Children=:=#{}, Terminating=:=#{} ->
        ok;
terminate(_Reason, #state{children=Children, terminating=Terminating}) when Children=:=#{} ->
        wait_children(Terminating);
terminate(_Reason, #state{spec=#child_spec{shutdown=Shutdown}, children=Children, terminating=Terminating}) ->
        do_terminate(Shutdown, Children, Terminating).

-spec code_change(_, _, _) -> _.
code_change(_OldVsn, State=#state{module=Mod, args=Args}, _Extra) ->
        case Mod:init(Args) of
                {ok, {Flags, Spec}} ->
                        case check_init(Flags, Spec) of
                                {ok, F, S} ->
                                        {ok, State#state{flags=F, spec=S}};
                                {error, Error} ->
                                        {error, {supervisor_data, Error}}
                        end;
                ignore ->
                        {ok, State};
                Error ->
                        Error
        end.

unlink_flush(Pid, DefaultReason) ->
        unlink(Pid),
        receive
                {'EXIT', Pid, Reason} ->
                        Reason
                after 0 ->
                        DefaultReason
        end.

do_terminate(Shutdown, Children, Terminating) ->
        Terminating1 = maps:fold(
                fun(Pid, {Mon, _}, Acc) ->
                        Timer = if
                                    Shutdown=:=brutal_kill ->
                                        exit(Pid, kill),
                                        undefined;
                                    Shutdown=:=infinity ->
                                        exit(Pid, shutdown),
                                        undefined;
                                    true ->
                                        exit(Pid, shutdown),
                                        erlang:start_timer(Shutdown, self(), {terminate_timeout, Pid})
                                end,
                        Acc#{Pid => {Mon, Timer, []}}
                end,
                Terminating,
                Children
        ),
        wait_children(Terminating1).

wait_children(Terminating) when Terminating=:=#{} ->
        ok;
wait_children(Terminating) ->
        receive
                {'CHILD-DOWN', Mon, process, Pid, Reason} when is_map_key(Pid, Terminating) ->
                        case maps:take(Pid, Terminating) of
                                {{Mon, Timer, ReplyTo}, Terminating1} ->
                                        maybe_cancel_timer(Timer),
                                        Reply = case unlink_flush(Pid, Reason) of
                                                    killed -> ok;
                                                    normal -> ok;
                                                    shutdown -> ok;
                                                    {shutdown, _} -> ok;
                                                    Reason1 -> {error, Reason1}
                                                end,
                                        reply_all(ReplyTo, Reply),
                                        wait_children(Terminating1);
                                _ ->
                                        wait_children(Terminating)
                        end;
                {timeout, Timer, {terminate_timeout, Pid}} when is_map_key(Pid, Terminating) ->
                        case Terminating of
                                #{Pid := {Mon, Timer, ReplyTo}} ->
                                        exit(Pid, kill),
                                        wait_children(Terminating#{Pid => {Mon, undefined, ReplyTo}});
                                _ ->
                                        wait_children(Terminating)
                        end
        end.

maybe_cancel_timer(undefined) ->
        ok;
maybe_cancel_timer(Timer) ->
        erlang:cancel_timer(Timer, [{async, true}, {info, false}]).

reply_all([ReplyTo|ReplyTos], Msg) ->
        gen_server:reply(ReplyTo, Msg),
        reply_all(ReplyTos, Msg);
reply_all([], _) ->
        ok.

start_child(M, F, A, Args) ->
        case catch erlang:apply(M, F, A++Args) of
                {ok, Pid} -> {ok, Pid};
                {ok, Pid, Extra} -> {ok, Pid, Extra};
                ignore -> ignore;
                {error, _} = Error -> Error;
                Other -> {error, Other}
        end.

check_init(SupFlags, ChildSpec) ->
        try
                {ok, check_sup_flags(SupFlags), check_child_spec(ChildSpec)}
        catch
                error:Error -> {error, Error}
        end.

check_sup_flags(#{} = SupFlags) ->
        Intensity = case SupFlags of
                        #{intensity := I} when is_integer(I), I>=0 -> I;
                        #{intensity := I} -> error({invalid_intensity, I});
                        #{} -> 1
                    end,
        Period = case SupFlags of
                     #{period := P} when is_integer(P), P>0 -> P;
                     #{period := P} -> error({invalid_period, P});
                     #{} -> 5
                 end,
        #sup_flags{intensity=Intensity,
                   period=Period};
check_sup_flags(SupFlags) ->
        error({invalid_sup_flags, SupFlags}).

check_child_spec(#{start := {M, F, A}} = ChildSpec) when is_atom(M), is_atom(F), is_list(A) ->
        {Type, DefaultShutdown} = case maps:get(type, ChildSpec, worker) of
                                      supervisor -> {supervisor, infinity};
                                      worker -> {worker, 5000};
                                      T -> error({invalid_type, T})
                                  end,
        Restart = case maps:get(restart, ChildSpec, temporary) of
                      temporary -> temporary;
                      transient -> transient;
                      R -> error({invalid_restart, R})
                  end,
        Shutdown = case maps:get(shutdown, ChildSpec, DefaultShutdown) of
                       infinity -> infinity;
                       brutal_kill -> brutal_kill;
                       S when is_integer(S), S>=0 -> S;
                       S -> error({invalid_shutdown, S})
                   end,
        Modules = case maps:get(modules, ChildSpec, [M]) of
                      dynamic -> dynamic;
                      [_]=Mod when is_atom(Mod) -> Mod;
                      Mod -> error({invalid_modules, Mod})
                  end,
        #child_spec{id=make_ref(),
                    module=M,
                    function=F,
                    args=A,
                    restart=Restart,
                    shutdown=Shutdown,
                    type=Type,
                    modules=Modules};
check_child_spec(ChildSpec) ->
        error({invalid_child_spec, ChildSpec}).
