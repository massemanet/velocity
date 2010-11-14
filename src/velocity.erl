%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created :  6 Nov 2010 by mats cronqvist <masse@kreditor.se>

%% @doc
%% @end

-module('velocity').
-author('mats cronqvist').

-export([cfg/1, cfg/2, cfg/3
         , delete/1,delete/3
         , find/3
         , insert/3]).

%% A schema-less, multi-keyed dictionary of fixed temporal size.
%%
%% Conceptually, the entries consist of a list of tagged tuples. The
%% entries are indexed by some number of the tagged tuples. There is
%% no primary key and no schema. Each insert can be of unique form.
%% Each insert will remove all entries that are "too old", i.e. older
%% than the timeout. The timeout is settable through the cfg function,
%% e.g. velocity:cfg(Tab,timeout,2) sets the timeout to 2 hours.
%%
%% In addition to the table ID, velocity:insert takes two arguments,
%% both of which are a list of tagged tuples. The elements of the
%% first list are indexed, the elements of the second list are
%% not. E.g. calling
%%
%% velocity:insert(Tab,[{a,"A"},{b,"B"}],[{c,"C"},{d,"D"}]).
%%
%% will allow you to find this entry by calling velocity:find(Tab,a,"A")
%% or velocity:find(Tab,b,"B").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% config

default(timeout) -> 1. %hours

%% set a cfg
cfg(Tab,Tag,Val) -> 
  assert_table(Tab),
  ets:insert(Tab,{{cfg,Tag},Val}).

%% query a cfg
cfg(Tab,Tag) ->
  assert_table(Tab),
  ets:lookup_element(Tab,{cfg,Tag},2).

%% list all cfg's
cfg(Tab) -> 
  assert_table(Tab),
  [{T,V} || [T,V] <- ets:match(Tab,{{cfg,'$1'},'$2'})].
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API

%% insert an entry and its indices, and prune the table
insert(Tab,Indices,Data) ->
  assert_table(Tab),
  prune(Tab),
  TS = now(),
  [ets:insert(Tab,{{Tag,Ind,TS}}) || {Tag,Ind} <- Indices],
  ets:insert(Tab,{TS,Indices,Data}).

% find an entry by an index and delete it (and its indices)
delete(Tab,Tag,Val) ->
  assert_table(Tab),
  case find_ts(Tab,Tag,Val) of
    [] -> true;
    TS -> delete(Tab,TS)
  end.

% delete an entry (and its indices)
delete(Tab,TS) ->
  assert_table(Tab),
  Indices = ets:lookup_element(Tab,TS,2),
  [ets:delete(Tab,{T,V,TS}) || {T,V} <- Indices],
  ets:delete(Tab,TS).

% delete the table
delete(Tab) ->
  catch ets:info(Tab,owner) ! quit.

% find an entry by an index
find(Tab,Tag,Val) ->
  assert_table(Tab),
  [[{ts,TS}|lookup(Tab,TS)] || TS <- find_ts(Tab,Tag,Val)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal

assert_table(Tab) ->
  case ets:info(Tab,owner) of
   undefined -> mk_table(Tab);
    _ -> ok
  end.

mk_table(Tab) ->
  {Pid,Ref} = spawn_monitor(mk_table(Tab,self())),
  receive
    {ok,Pid}              -> erlang:demonitor(Ref,[flush]);
    {'DOWN',Ref,_,_,Info} -> exit({error_creating,Tab,Info})
  end.
  
mk_table(Tab,Daddy) ->
  fun() ->
      ets:new(Tab,[named_table,ordered_set,public]),
      cfg(Tab,timeout,default(timeout)),
      Daddy ! {ok,self()},
      receive quit -> ok end
  end.

find_ts(Tab,Tag,Val) ->
  [TS || [TS] <- ets:match(Tab,{{Tag,Val,'$1'}})].

%delete all entries that have timed out, i.e. are older than N hours.
prune(Tab) ->
  Then = mk_then(cfg(Tab,timeout)),
  TSs = ets:select(Tab,[{{'$1','_','_'},[{'<','$1',{Then}}],['$1']}]),
  length([delete(Tab,TS) || TS <- TSs]).

%return now() minus N hours
mk_then(N) -> 
  mk_then(now(),N*3600).

% handle any TS newer the Beginning of Time, i.e. Jan 1, 1970.
mk_then({MS,S,_},Diff) when Diff=<S -> {MS,S-Diff,0};
mk_then({MS,S,_},Diff) -> mk_then({MS-1,S+1000000,0},Diff).

lookup(Tab,TS) ->
  [{TS,I,D}] = ets:lookup(Tab,TS),
  I++D.
