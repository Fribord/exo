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
%%% @author Marina Westman Lonne <malotte@malotte.net>
%%% @copyright (C) 2016, Tony Rogvall
%%% @doc
%%%   Simple exo_mqtt_server
%%%
%%% Created: May 2016 by Marina Westman Lonne
%%% @end


-module(exo_mqtt_server).
-behaviour(exo_socket_server).

-include("exo.hrl").
-include("exo_socket.hrl").
-include("../include/exo_mqtt.hrl").

%% exo_socket_server callbacks
-export([init/2,
	 data/3,
	 info/3,
	 close/2,
	 error/3,
	 control/4]).

%% configurable start
-export([start/2,
	 start_link/2,
	 stop/1]).

%% mqtt specific auth-handling
-export([handle_creds/3]).

%% send on socket
-export([publish/3]).

%% applied function
-export([handle_mqtt/2]).  

-record(ctx,
	{
	  state = init::init | connected | disconnected,
	  client::binary() | undefined,
	  keep_alive = 0::integer(),
	  keep_alive_timer::reference() | undefined,
	  topics = []::list(),
	  access::list(access()),
	  mqtt_handler::term(), 
	  handler_ctx::term()
	}).

%%-----------------------------------------------------------------------------
%% @doc
%%  Starts a socket server on port Port with server options ServerOpts
%% that are sent to the server when a connection is established,
%% i.e init is called.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec start(Port::integer(),
	    ServerOptions::list({Option::atom(), Value::term()})) ->
		   {ok, ChildPid::pid()} |
		   {error, Reason::term()}.

start(Port, Options) ->
    do_start(start, Port, Options).

%%-----------------------------------------------------------------------------
%% @doc
%%  Starts and links a socket server on port Port with server options ServerOpts
%% that are sent to the server when a connection is established,
%% i.e init is called.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec start_link(Port::integer(),
		 ServerOptions::list({Option::atom(), Value::term()})) ->
			{ok, ChildPid::pid()} |
			{error, Reason::term()}.

start_link(Port, Options) ->
    do_start(start_link, Port, Options).


do_start(Start, Port, Options) ->
    lager:debug("exo_mqtt_server: ~w: port ~p, server options ~p",
	   [Start, Port, Options]),
    {SessionOptions,Options1} = 
	exo_lib:split_options([mqtt_handler, access],Options),
    Dir = code:priv_dir(exo),
    Access = proplists:get_value(access, Options, []),
    case exo_lib:validate_access(Access) of
	ok ->
	    exo_socket_server:Start(Port, 
				    [tcp,probe_ssl,mqtt],
				    [{active,once},{reuseaddr,true},
				     {verify, verify_none},
				     {keyfile, filename:join(Dir, "host.key")},
				     {certfile, filename:join(Dir, "host.cert")}
				     | Options1],
				    ?MODULE, SessionOptions);
	E -> E
    end.

%%-----------------------------------------------------------------------------
%% @doc
%%  Stops the socket server.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec stop(Pid::pid()) ->
		   {ok, ChildPid::pid()} |
		   {error, Reason::term()}.
stop(Pid) ->
    exo_socket_server:stop(Pid).

%%-----------------------------------------------------------------------------
%% @doc
%%  Init function called when a connection is established.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec init(Socket::#exo_socket{},
	   ServerOptions::list({Option::atom(), Value::term()})) ->
		  {ok, Ctx::#ctx{}}.

init(Socket, Options) ->
    lager:debug("exo_mqtt_server: connection on: ~p ", [Socket]),
    {ok, _PeerName} = exo_socket:peername(Socket),
    {ok, _SockName} = exo_socket:sockname(Socket),
    lager:debug("exo_mqtt_server: connection from peer: ~p, sockname: ~p,\n"
		"options ~p", [_PeerName, _SockName, Options]),
    Access = proplists:get_value(access, Options, []),
    MH = proplists:get_value(mqtt_handler, Options, undefined),
    {ok, #ctx{access = Access, mqtt_handler = MH}}.

%% To avoid a compiler warning. Should we actually support something here?
%%-----------------------------------------------------------------------------
%% @doc
%%  Control function - not used.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec control(Socket::#exo_socket{},
	      Request::term(), From::term(), Ctx::#ctx{}) ->
		     {ignore, Ctx::#ctx{}}.

control(_Socket, _Request, _From, Ctx) ->
    {ignore, Ctx}.

%%-----------------------------------------------------------------------------
%% @doc
%%  Data function called when data is received.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec data(Socket::#exo_socket{},
	   Data::term(),
	   Ctx::#ctx{}) ->
		  {ok, NewCtx::#ctx{}} |
		  {stop, {error, Reason::term()}, NewCtx::#ctx{}}.

data(Socket, Data, Ctx) ->
    lager:debug("data = ~w", [Data]),
    case Data of
	<<Type:4, Duplicate:1, QoS:2, Retain:1, Rest/binary>> ->
	    lager:debug("mqtt ~p, data: ~p", [Type, Rest]),
	    handle_mqtt(Socket,
			#mqtt_header {type = Type,
				      duplicate = Duplicate,
				      qos = QoS,
				      retain = Retain},
			parse_mqtt(Rest),
			keep_alive(Ctx));
	_ when is_list(Data); is_binary(Data) ->
	    lager:debug("unknown mqtt msg: ~p", [Data]),
	    {stop, {error, sync_error}, Ctx};
	Error ->
	    {stop, Error, Ctx}
    end.

%%-----------------------------------------------------------------------------
%% @doc
%%  Info function called when info is received.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec info(Socket::#exo_socket{},
	   Info::term(),
	   Ctx::#ctx{}) ->
		  {ok, NewCtx::#ctx{}} |
		  {stop, {error, Reason::term()}, NewCtx::#ctx{}}.

info(_Socket, {timeout, T, keep_alive} = _Info, 
     Ctx=#ctx {keep_alive_timer = T}) ->
    lager:debug("info = ~w", [_Info]),
    lager:debug("keep_alive_failed"),
    {stop, normal, Ctx};

info(_Socket, {timeout, _T, keep_alive} = _Info, Ctx) ->
    lager:debug("info = ~w", [_Info]),
    lager:debug("unhandled timeout", []),
    {ok, Ctx};

info(Socket, Info, Ctx) ->
    lager:debug("info = ~w", [Info]),
    handle_info(Socket, Info, Ctx),
    {ok, Ctx}.

%%-----------------------------------------------------------------------------
%% @doc
%%  Close function called when a connection is closed.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec close(Socket::#exo_socket{},
	    Ctx::#ctx{}) ->
		   {ok, NewCtx::#ctx{}}.

close(_Socket, Ctx) ->
    lager:debug("close", []),
    {ok,Ctx}.

%%-----------------------------------------------------------------------------
%% @doc
%%  Error function called when an error is detected.
%%  Stops the server.
%%
%% @end
%%-----------------------------------------------------------------------------
-spec error(Socket::#exo_socket{},
	    Error::term(),
	    Ctx::#ctx{}) ->
		   {stop, {error, Reason::term()}, NewCtx::#ctx{}}.

error(_Socket,Error,Ctx) ->
    lager:debug("exo_mqtt_serber: error = ~p", [Error]),
    {stop, Error, Ctx}.


%%-----------------------------------------------------------------------------
%% @doc
%% Publish an mqtt message
%%
%% @end
%%-----------------------------------------------------------------------------
-spec publish(Socket::#exo_socket{}, Topic::binary() | string(), Msg::term()) ->
		     ok.

publish(Socket, Topic, Msg) ->
    lager:debug("topic ~p, message ~p", [Topic, Msg]),
    Publish = exo_mqtt:make_packet(#mqtt_header{type = ?MQTT_PUBLISH},
				   Topic, Msg),
    exo_socket:send(Socket, Publish),
    ok.


%%-----------------------------------------------------------------------------
%% Internal functions
%%-----------------------------------------------------------------------------
handle_mqtt(_Socket, _Header, {error, Error}, Ctx) ->
    lager:error("parsing of message failed with reason ~p", [Error]),
    {stop, normal, Ctx};
handle_mqtt(_Socket, #mqtt_header{qos = QoS}, _Packet, Ctx) 
  when QoS > 1 ->
    lager:warning("unhandled qos ~p in ~p", [QoS, Ctx]),
    {ok, Ctx};
handle_mqtt(_Socket, _Header=#mqtt_header{type = ?MQTT_CONNECT}, 
	    _Packet, Ctx=#ctx {state = connected}) ->
    lager:error("illegal connect when already connected"),
    {stop, normal, Ctx};
handle_mqtt(Socket, Header=#mqtt_header{type = ?MQTT_CONNECT}, 
	    Packet, Ctx=#ctx {state = State}) 
  when State =/= connected ->
    lager:debug("connect ~p", [Packet]),
    handle_connect(Socket, Header, Packet, Ctx);
handle_mqtt(_Socket, #mqtt_header{type = _Type}, 
	    Packet, Ctx=#ctx {state = State}) 
  when State =/= connected ->
    lager:debug("unexpected ^p ~p in ~p", [_Type, Packet, Ctx]),
    %% Must be connected to handle other messages than connect
    {ok, Ctx};
handle_mqtt(Socket, Header=#mqtt_header{type = ?MQTT_DISCONNECT}, 
	    Packet=#mqtt_packet{length = 0, bin = <<>>}, Ctx) ->
    lager:debug("disconnect ~p", [Packet]),
    handle_disconnect(Socket, Header, Packet, Ctx);
handle_mqtt(Socket, Header=#mqtt_header{type = ?MQTT_PINGREQ},
	    Packet=#mqtt_packet{length = 0, bin = <<>>}, Ctx) ->
    lager:debug("ping ~p", [Packet]),
    send_response(Socket, Header, ?MQTT_PINGRESP, <<>>, <<>>),
    {ok, Ctx};
handle_mqtt(Socket, Header=#mqtt_header{type = ?MQTT_SUBSCRIBE}, 
	    Packet, Ctx) ->
    lager:debug("subscribe ~p", [Packet]),
    handle_subscribe(Socket, Header, Packet, Ctx);
handle_mqtt(Socket, Header=#mqtt_header{type = ?MQTT_UNSUBSCRIBE}, 
	    Packet, Ctx) ->
    lager:debug("unsubscribe ~p", [Packet]),
    handle_unsubscribe(Socket, Header, Packet, Ctx);
handle_mqtt(Socket, Header=#mqtt_header{type = ?MQTT_PUBLISH}, 
	    Packet, Ctx) ->
    lager:debug("publish ~p", [Packet]),
    handle_publish(Socket, Header, Packet, Ctx);
handle_mqtt(_Socket, _Header=#mqtt_header{type = ?MQTT_PUBACK}, 
	    Packet, Ctx) ->
    lager:debug("publish ack ~p, ignored", [Packet]),
    {ok, Ctx};
handle_mqtt(_Socket, #mqtt_header{type = _Type}, _Packet, Ctx) ->
    lager:debug("unexpected ~p: ~p in ~p", [_Type,_Packet, Ctx]),
    {ok, Ctx}.

%%-----------------------------------------------------------------------------
handle_connect(Socket, Header, Packet, Ctx) ->
    case verify_connect(Packet, Ctx) of
	{UserName, PassWord, NewCtx} ->
	    do_connect(Socket, Header, UserName, PassWord, NewCtx);
	ignore ->
	    {ok, Ctx};
	stop ->
	    {stop, normal, Ctx};
	{error, Code} ->
	    send_response(Socket, Header, ?MQTT_CONNACK, {0, Code}, <<>>),
	    {stop, normal, Ctx}
    end.

verify_connect(_Packet=#mqtt_packet{length = L, bin = Bin}, Ctx) ->
    case Bin of
	<<FrameBin:L/binary, _Rest/binary>> ->
            {ProtocolName, Rest1} = parse_field(FrameBin, 1),
            <<_:4, ProtocolVersion:4, Rest2/binary>> = Rest1,
            <<UsernameFlag : 1,
              PasswordFlag : 1,
              WillRetain   : 1,
              WillQoS      : 2,
              WillFlag     : 1,
              CleanSession : 1,
              Reserved    : 1,
              KeepAlive    : 16/big,
              Rest3/binary>>   = Rest2,
            {ClientId,  Rest4} = parse_field(Rest3, 1),
            {_WillTopic, Rest5} = parse_field(Rest4, WillFlag),
            {_WillMsg,   Rest6} = parse_field(Rest5, WillFlag),
            {UserName,  Rest7} = parse_field(Rest6, UsernameFlag),
            {PassWord, <<>>}  = parse_field(Rest7, PasswordFlag),
	    lager:debug("client ~p", [ClientId]),
	    lager:debug("keep_alive ~p", [KeepAlive]),
	    lager:debug("user ~p, password ~p", [UserName, PassWord]),
	    if ProtocolName =/= ?MQTT ->
		    lager:error("protocol not mqtt ~p", [ProtocolName]),
		    stop;
	       ProtocolVersion =/= ?MQTT_VERSION ->
		    lager:error("illegal protocol version ~p", 
				[ProtocolVersion]),
		    {error, ?MQTT_CONNACK_PROTO_VER};
	       Reserved =/= 0 ->
		    lager:error("wrong reserve flag in connect ~p", 
				[_Packet]),
		    stop;
	       WillFlag =/= 0; WillQoS =/= 0; WillRetain =/= 0 ->
		    lager:debug("will message not implemented yet"),
		    ignore; %% ???
	       CleanSession =:= 0, ClientId =:= <<>> ->
		    lager:error("not possible to resume for unknown client"),
		    {error, ?MQTT_CONNACK_INVALID_ID};
	       true -> 
		    {UserName, PassWord, 
		     session(CleanSession, ClientId,
			     keep_alive(Ctx#ctx {keep_alive = KeepAlive}))}
	    end;
	_ ->
 	    lager:warning("faulty message in ~p", [Ctx]),
	    ignore %% or stop ???
    end.

session(0, Client, Ctx=#ctx {client = Client}) -> Ctx;
session(_Clean, Client, Ctx) -> Ctx#ctx {topics = [], client = Client}. 

keep_alive(Ctx=#ctx {keep_alive_timer = Timer}) 
  when Timer =/= undefined ->
    lager:debug("cancel keep alive timer", []),
    erlang:cancel_timer(Timer),
    keep_alive(Ctx#ctx {keep_alive_timer = undefined});
keep_alive(Ctx=#ctx {keep_alive = 0}) ->
    lager:debug("no keep alive timer", []),
    Ctx;
keep_alive(Ctx=#ctx {keep_alive = KeepAlive, keep_alive_timer = undefined}) ->
    lager:debug("start keep alive timer", []),
    %% According to the spec timeout should be at 1,5 * KeepAlive,
    %% However, when testig with eqmttc we have seen that it can delay 
    %% pinging to almost 2 * KeepAlive so we adjust to that.
    %% Also adding a margin of 5 secs.
    TimeOut = (KeepAlive * 2) + 5,
    %% keepalive s -> ms
    T = erlang:start_timer(TimeOut * 1000 , self(), keep_alive), 
    Ctx#ctx{keep_alive_timer = T}.
    
do_connect(Socket, Header, User, Pass, Ctx=#ctx {access = Access}) ->
    lager:debug("access ~p", [Access]),
    case exo_lib:handle_access(Access, Socket, 
			       {?MODULE, handle_creds, [User, Pass]}) of
	ok ->
	    case to_handler(Socket, {connect, User, Pass}, Ctx) of
		{ok, NewCtx} ->
		    send_response(Socket, Header, ?MQTT_CONNACK, 
				  {0, ?MQTT_CONNACK_ACCEPT}, <<>>),
		    {ok, NewCtx#ctx {state = connected}};
		_Other ->
		    lager:warning("handler connect result ~p in ctx ~p",  
				  [_Other, Ctx]),
		    {ok, Ctx}
		end;
	{error, unauthorised} ->
	    send_response(Socket, Header, ?MQTT_CONNACK, 
			  {0, ?MQTT_CONNACK_AUTH}, <<>>),
		    {ok, Ctx}
    end.

handle_creds([], _User, _Pass) -> 
    {error, unauthorised};
handle_creds([{basic, _Path, User, Pass, _Realm} = _Cred | _Rest], User,  Pass) ->
    lager:debug("match ~p", [_Cred]),
    ok;
handle_creds([_Cred | Rest], User, Pass) ->
    lager:debug("no match ~p", [_Cred]),
    handle_creds(Rest, User, Pass).
	   
handle_disconnect(Socket, _Header, _Packet, Ctx) ->
    to_handler(Socket, disconnect, Ctx),
    {stop, normal, Ctx#ctx {state = disconnected}}.

%%-----------------------------------------------------------------------------
handle_subscribe(Socket, Header, Packet, Ctx) ->
    lager:debug("subscribe ~p", [Packet]),
    case parse_packet(Packet) of
	{PacketId, Rest} ->
  	    TopicsAndQoS = exo_mqtt:parse_topics(?MQTT_SUBSCRIBE, Rest),
	    %% QoS ignored ???
	    Topics = [Topic || {Topic, _Qos} <- TopicsAndQoS],
	    lager:debug("topics ~p", [Topics]),
	    do_subscribe(Socket, Header, Topics, PacketId, Ctx);
	_ ->
	    lager:warning("faulty message in ~p", [Ctx]),
	    {ok, Ctx} %% or stop ???
   end.

do_subscribe(Socket, Header, Topics, PacketId, 
	     Ctx=#ctx {topics = OldTopics}) ->
    case to_handler(Socket, {subscribe, Topics}, Ctx) of
	{{ok, ResultList}, NewCtx} when is_list(ResultList) ->
	    send_response(Socket, Header, ?MQTT_SUBACK, PacketId, ResultList),
	    {ok, NewCtx#ctx {topics = Topics ++ OldTopics}};
	_Other ->
	    lager:warning("handler subscribe result ~p in ctx ~p",  
			  [_Other, Ctx]),
	    {ok, Ctx}
    end.

%%-----------------------------------------------------------------------------
handle_unsubscribe(Socket, Header, Packet, Ctx) ->
    case parse_packet(Packet) of
	{PacketId, Rest} ->
  	    Topics = exo_mqtt:parse_topics(?MQTT_UNSUBSCRIBE, Rest),
	    lager:debug("topics ~p", [Topics]),
	    do_unsubscribe(Socket, Header, Topics, PacketId, Ctx);
	_ ->
	    lager:warning("faulty message in ~p", [Ctx]),
	    {ok, Ctx}
   end.
		    
 
do_unsubscribe(Socket, Header, Topics, PacketId, 
	       Ctx=#ctx {topics = OldTopics}) ->
    case to_handler(Socket, {unsubscribe, Topics}, Ctx) of
	{ok, NewCtx} ->
	    send_response(Socket, Header, ?MQTT_UNSUBACK, PacketId, <<>>),
	    {ok, NewCtx#ctx {topics = OldTopics -- Topics}};
	_Other ->
	    lager:warning("handler unsubscribe result ~p in ctx ~p",  
			  [_Other, Ctx]),
	    {ok, Ctx}
    end.

%%-----------------------------------------------------------------------------
handle_publish(_Socket, _Header=#mqtt_header{duplicate = 1}, _Packet, Ctx) ->
    lager:debug("resending not implemented yet"),
    {ok, Ctx};
handle_publish(_Socket, _Header=#mqtt_header{retain = 1}, _Packet, Ctx) ->
    lager:debug("retain not implemented yet"),
    {ok, Ctx};
handle_publish(Socket, Header=#mqtt_header{qos = 0}, 
	       _Packet=#mqtt_packet{length = L, bin = Bin}, Ctx) ->
    case Bin of
	<<FrameBin:L/binary, _Rest/binary>> ->
            {Topic, PayLoad} = parse_field(FrameBin, 1),
	    do_publish(Socket, Header, 0, Topic, PayLoad, Ctx);
	_ ->
	    lager:warning("faulty message in ~p", [Ctx]),
	    {ok, Ctx} %% or stop ??
    end;
handle_publish(Socket, Header, 
	       _Packet=#mqtt_packet{length = L, bin = Bin}, Ctx) ->
    case Bin of
	<<FrameBin:L/binary, _Rest/binary>> ->
            {Topic, Rest1} = parse_field(FrameBin, 1),
	    <<PacketId:16/big, PayLoad/binary>> = Rest1,
	    do_publish(Socket, Header, PacketId, Topic, PayLoad, Ctx);
	_ ->
	    lager:warning("faulty message in ~p", [Ctx]),
	    {ok, Ctx} %% or stop ??
    end.


do_publish(Socket, Header, PacketId, Topic, PayLoad, Ctx)->
    send_response(Socket, Header, ?MQTT_PUBACK, PacketId, <<>>),
    case to_handler(Socket, {publish, Topic, PayLoad}, Ctx) of
	{ok, NewCtx} ->
	    {ok, NewCtx};
	{{ok, {ResponseTopic, Data}}, NewCtx} ->
	    publish(Socket, ResponseTopic, Data, Ctx),
	    {ok, NewCtx};
	{_Other, NewCtx} ->
	    lager:warning("handler publish result ~p in ~p",  
			  [_Other, NewCtx]),
	    {ok, NewCtx}
    end.

%%-----------------------------------------------------------------------------
handle_info(Socket, Info, Ctx) ->
    case to_handler(Socket, {info, Info}, Ctx) of
	{ok, NewCtx} ->
	    {ok, NewCtx};
	_Other ->
	    lager:warning("handler info result ~p in ~p",  [_Other, Ctx]),
	    {ok, Ctx}
    end.
   

%%-----------------------------------------------------------------------------
to_handler(Socket, Data, Ctx=#ctx {mqtt_handler = MH, handler_ctx = Hctx}) ->
    %% Send to handler
    {M, F, As} = mqtt_handler(MH, [Socket, Data, Hctx]),
    try apply(M, F, As) of
	ok ->
	    {ok, Ctx};
	{ok, NewHctx} -> 
	    {ok, Ctx#ctx {handler_ctx = NewHctx}};
	{{ok, _Result} = Reply, NewHctx} -> 
	    lager:debug("reply ~p", [Reply]),
	    {Reply, Ctx#ctx {handler_ctx = NewHctx}};
	stop -> 
	    {stop, normal, Ctx};
	{{error, Error}, NewHctx}  ->  
	    lager:error("call to mqtt_handler ~p returned ~p",[MH, Error]),
	    {ok, Ctx#ctx {handler_ctx = NewHctx}}
    catch error:_E ->
	    lager:error("call to mqtt_handler ~p failed, reason ~p, stack ~p",
			[MH, _E, erlang:get_stacktrace()]),
	    {ok, Ctx} %% or stop ??
    end.
 
%%-----------------------------------------------------------------------------
publish(Socket, Topic, Data, #ctx {topics = Topics}) ->
    case lists:member(Topic, Topics) of
	true -> 
	    publish(Socket, Topic, Data);
	false ->
	    lager:warning("not publishing unsubscribed topic ~p",[Topic])
    end.
  
%%-----------------------------------------------------------------------------
send_response(_Socket, _Header, Type, <<>>, _PayLoad)
  when Type =/= ?MQTT_PINGRESP ->
    lager:debug("no ack when packet id undefined"),
    ok;
send_response(_Socket, _Header=#mqtt_header{qos = 0}, ?MQTT_PUBACK, 
	      _PacketId, _PayLoad) ->
    lager:debug("no puback when qos = 0"),
    ok;
send_response(Socket, Header, Type, PacketId, PayLoad) ->
    lager:debug("sending ack ~p", [Type]),
    Response = exo_mqtt:make_packet(Header#mqtt_header{type = Type},
				    PacketId, PayLoad), 
    exo_socket:send(Socket, Response).
   

%%-----------------------------------------------------------------------------
%% Support functions
%%-----------------------------------------------------------------------------
%% @private
mqtt_handler(undefined, Args) ->
    {?MODULE, handle_mqtt, Args};
mqtt_handler(Module, Args) when is_atom(Module) ->
    {Module, handle_mqtt, Args};
mqtt_handler({Module, Function}, Args) ->
    {Module, Function, Args};
mqtt_handler({Module, Function, XArgs}, Args) ->
    {Module, Function, Args ++ [XArgs]}.

%% @private
handle_mqtt(_Socket, _Topic) ->
    lager:debug("Topic ~p ignored", [_Topic]),
    ok.


%% @private
parse_mqtt(Bin) ->
    parse_mqtt(Bin, #mqtt_packet{}, 1, 0, ?MQTT_MAX_LEN).

parse_mqtt(_Bin, _MqttPacket, _Mutltiplier, Length, Max)
  when Length > Max ->
    {error, too_long};
parse_mqtt(<<>>, MqttPacket, _Mutltiplier, Length, _Max) ->
    MqttPacket#mqtt_packet{length = Length};
parse_mqtt(<<0:1, 2:7, Rest/binary>>, MqttPacket, 1, 0, _Limit) ->
    MqttPacket#mqtt_packet{length = 2, bin = Rest};
parse_mqtt(<<0:8, Rest/binary>>, MqttPacket, 1, 0, _Limit) ->
    MqttPacket#mqtt_packet{length = 0, bin = Rest};
parse_mqtt(<<1:1, Len:7, Rest/binary>>, MqttPacket, Multiplier, Length, Max) ->
    parse_mqtt(Rest, MqttPacket, Multiplier * ?MQTT_HIGHBIT, 
	       Length + Len * Multiplier, Max);
parse_mqtt(<<0:1, Len:7, Rest/binary>>, MqttPacket,  Multiplier, Length, Max) ->
    FrameLength = Length + Len * Multiplier,
    if
        FrameLength > Max -> {error, too_long};
        true -> MqttPacket#mqtt_packet{length = FrameLength, bin = Rest}
    end.
    
parse_packet(Packet=#mqtt_packet{length = L, bin = Bin}) ->
    case Bin of
	<<FrameBin:L/binary, _Rest/binary>> ->
           <<PacketId:16/big, Rest1/binary>> = FrameBin,
	    {PacketId, Rest1};
	_ ->
	    lager:warning("not able to parse packet ~p",[Packet]),
	    Bin
    end.

parse_field(Bin, 0) -> {<<>>, Bin};
parse_field(Bin, _Flag) -> exo_mqtt:parse_field(Bin).

