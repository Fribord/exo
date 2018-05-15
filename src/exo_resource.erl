%%% coding: latin-1
%%%---- BEGIN COPYRIGHT -------------------------------------------------------
%%%
%%% Copyright (C) 2016, Rogvall Invest AB, <tony@rogvall.se>
%%%
%%% This software is licensed as described in the file COPYRIGHT, which
%%% you should have received as part of this distribution. The terms
%%% are also available at http://www.rogvall.se/docs/copyright.txt.
%%%
%%% You may opt to use, copy, modify, merge, publish, distribute and/or sell
%%% copies of the Software, and permit persons to whom the Software is
%%% furnished to do so, under the terms of the COPYRIGHT file.
%%%
%%% This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
%%% KIND, either express or implied.
%%%
%%%---- END COPYRIGHT ---------------------------------------------------------
%%% @author Malotte W Lönne <malotte@malotte.net>
%%% @doc
%%%    exo resource api
%%%
%%% Created : June 2016 by Malotte W Lönne
%%% @end
-module(exo_resource).

%% general api
-export([start_link/1, 
	 stop/0]).

%% functional api
-export([acquire/1,
	 acquire/2,
	 acquire_async/2,
	 release/1,
	 transfer/2,
	 calc_avail/0]).

%% test api
-export([dump/0,
	 avail/1,
	 avail/0]).

-export([port_limit/0]).
-export([port_count/0]).

-define(SERVER, exo_resource_srv).
-define(RESERVED_PORTS, 30). 
-define(RESERVED_FDS, 20). 
-define(DEFAULT_MAX_PORTS, 1024).  %% fixme

%% For dialyzer
-type start_options()::{linked, TrueOrFalse::boolean()}.

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% Loads configuration from File.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Opts::list(start_options())) -> 
			{ok, Pid::pid()} | 
			ignore | 
			{error, Error::term()}.

start_link(Opts) ->
    F =	case proplists:get_value(linked,Opts,true) of
	    true -> start_link;
	    false -> start
	end,
    gen_server:F({local, ?SERVER}, ?SERVER, Opts, []).


%%--------------------------------------------------------------------
%% @doc
%% Stops the server.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Error::term()}.

stop() ->
    gen_server:call(?SERVER, stop).


%%--------------------------------------------------------------------
%% @doc
%% Requests a resource and waits for the reply.
%%
%% @end
%%--------------------------------------------------------------------
-spec acquire(Timeout::timeout()) -> 
		    {resource, ok, Ref::term()} | 
		    {resource, error, Error::atom()}.

acquire(Timeout) 
  when (is_integer(Timeout) andalso Timeout > 0) orelse
       Timeout =:= infinity ->
    Ref = make_ref(),
    acquire(Ref, Timeout).
	
%%--------------------------------------------------------------------
%% @doc
%% Requests a namned resource and waits for the reply.
%%
%% @end
%%--------------------------------------------------------------------
-spec acquire(Ref::term(), Timeout::timeout()) -> 
		    {resource, ok, Ref::term()} | 
		    {resource, error, Error::atom()}.

acquire(Ref, Timeout) 
  when (is_integer(Timeout) andalso Timeout > 0) orelse
       Timeout =:= infinity ->
    gen_server:cast(?SERVER, {acquire, {self(), Ref}, Timeout}),
    T = if Timeout =:= infinity -> Timeout;
	   true -> Timeout + 1000
	end,
    receive
	{resource, _Result, _} = Reply -> Reply
    after
	T -> {resouce, error, not_available}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Requests a resource and returns without waiting for reply.
%%
%% @end
%%--------------------------------------------------------------------
-spec acquire_async(Ref::term(), Timeout::timeout()) -> 
			  ok | 
			  {error, Error::atom()}.

acquire_async(Ref, Timeout) 
  when (is_integer(Timeout) andalso Timeout > 0) orelse
       Timeout =:= infinity ->
    gen_server:cast(?SERVER, {acquire, {self(), Ref}, Timeout}).

%%--------------------------------------------------------------------
%% @doc
%% Release a resource
%%
%% @end
%%--------------------------------------------------------------------
-spec release(Ref::term()) -> 
		     ok | {error, Error::atom()}.

release(Ref) ->
    gen_server:cast(?SERVER,{release, {self(), Ref}}).
	
%%--------------------------------------------------------------------
%% @doc
%% Transfer a resource
%%
%% @end
%%--------------------------------------------------------------------
-spec transfer(Ref::term(), NewPid::pid()) -> 
		     ok | {error, Error::atom()}.

transfer(Ref, NewPid) 
  when is_pid(NewPid) ->
    gen_server:cast(?SERVER,{transfer, {self(), Ref}, NewPid}).
	
%%--------------------------------------------------------------------
%% @doc
%% Dumps data to standard output.
%%
%% @end
%%--------------------------------------------------------------------
-spec dump() -> ok | {error, Error::atom()}.

dump() ->
    gen_server:call(?SERVER,dump).

%%--------------------------------------------------------------------
%% @doc
%% Changes avail.
%%
%% @end
%%--------------------------------------------------------------------
-spec avail(I::integer()) -> ok | {error, Error::atom()}.

avail(I) when is_integer(I) ->
    gen_server:call(?SERVER, {avail, I}).

%%--------------------------------------------------------------------
%% @doc
%% Restores avail.
%%
%% @end
%%--------------------------------------------------------------------
-spec avail() -> ok | {error, Error::atom()}.

avail()  ->
    gen_server:call(?SERVER, {avail, calc_avail()}).


%%--------------------------------------------------------------------
%% @doc
%% Calculates available resources.
%%
%% @end
%%--------------------------------------------------------------------
-spec calc_avail() -> Avail::integer() | {error, Error::atom()}.

calc_avail() ->
    MaxPorts = port_limit() - port_count(),
    ReservedPorts = max(trunc(0.1 * MaxPorts), ?RESERVED_PORTS),
    case max_fds() of
	MaxFds when is_integer(MaxFds) ->
	    ReservedFds = max(trunc(0.1 * MaxFds), ?RESERVED_FDS),
	    ErlangPorts = [erlang:port_info(Port, name) || 
			      Port <- erlang:ports()],
	    ErlangFds = [Name || {name, Name} <- ErlangPorts, 
			 (Name =:= "efile" orelse Name =:= "tcp_inet" orelse
			  Name =:= "udp_inet" orelse Name =:= "afunix_drv")],
	    min(MaxPorts - ReservedPorts, 
		MaxFds - ReservedFds - length(ErlangFds));
	{error, _Reason} = E ->
	    lager:debug("calc_avail failed, reason ~p",[_Reason]),
	    E
    end.

port_limit() ->
    try erlang:system_info(port_limit)
    catch error:_ -> ?DEFAULT_MAX_PORTS
    end.

port_count() ->
    try erlang:system_info(port_count)
    catch error:_ -> length(erlang:ports())
    end.

max_fds() ->
    case proplists:get_value(max_fds, erlang:system_info(check_io)) of
	I when is_integer(I) -> I;
	undefined -> ulimit_fds()
    end.

ulimit_fds() ->
    case string:tokens(os:cmd("ulimit -n"), "\n") of
	[Fds] ->
	    try list_to_integer(Fds)
	    catch 
		error:_ ->   {error, list_to_integer_failed}
	    end;
	_Other ->
	    lager:error("ulimit result ~p",[_Other]),
	    {error, ulimit_failed}
    end.
