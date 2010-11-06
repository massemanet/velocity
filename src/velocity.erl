%% -*- mode: erlang; erlang-indent-level: 2 -*-
%%% Created :  6 Nov 2010 by mats cronqvist <masse@kreditor.se>

%% @doc
%% @end

-module('velocity').
-author('mats cronqvist').

-export([cfg/0, cfg/1, cfg/2
         , insert/2
         , find/2
         , delete/0
         , delete/2]).

%% A schema-less, multi-keyed dictionary of fixed temporal size.
%%
%% Conceptually, the entries consist of a list of tagged tuples. The
%% entries are indexed by some number of the tagged tuples. There is
%% no primary key and no schema. Each insert can be of unique form.
%% Each insert will remove all entries that are "too old", i.e. older
%% than the timeout. The timeout is settable through the cfg function,
%% e.g. velocity:cfg(timeout,2) sets the timeout to 2 hours.
%%
%% velocity:insert takes two arguments, both of which are a list of
%% tagged tuples. The elements of the first list are indexed, the
%% elements of the second list are not. E.g. calling
%%
%% velocity:insert([{a,"A"},{b,"B"}],[{c,"C"},{d,"D"}]).
%%
%% will allow you to find this entry by calling velocity:find(a,"A")
%% or velocity:find(b,"B").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% config

default(timeout) -> 1. %hours

cfg(Tag,Val) -> ets:insert(?MODULE,{{cfg,Tag},Val}).

cfg(Tag) -> ets:lookup_element(?MODULE,{cfg,Tag},2).

cfg() -> [{T,V} || [T,V] <- ets:match(?MODULE,{{cfg,'$1'},'$2'})].
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API

%% insert an entry and its indices, and prune the table
insert(Indices,Data) ->
  assert_table(),
  prune(),
  TS = now(),
  [ets:insert(?MODULE,{{Tag,Ind,TS}}) || {Tag,Ind} <- Indices],
  ets:insert(?MODULE,{TS,Indices,Data}).

% find an entry by an index and delete it (and its indices)
delete(Tag,Val) ->
  assert_table(),
  case find_ts(Tag,Val) of
    [] -> true;
    TS -> delete(TS)
  end.

% delete an entry (and its indices)
delete(TS) ->
  Indices = ets:lookup_element(?MODULE,TS,2),
  [ets:delete(?MODULE,{T,V,TS}) || {T,V} <- Indices],
  ets:delete(?MODULE,TS).

% delete the table
delete() ->
  catch ets:info(?MODULE,owner) ! quit.

% find an entry by an index
find(Tag,Val) ->
  assert_table(),
  [lookup(TS) || TS <- find_ts(Tag,Val)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal

assert_table() ->
  case ets:info(?MODULE,owner) of
   undefined -> mk_table();
    _ -> ok
  end.

mk_table() ->
  {Pid,Ref} = spawn_monitor(mk_table(self())),
  receive
    {ok,Pid} -> erlang:demonitor(Ref,[flush]);
    {'DOWN',Ref,_,_,Info} -> exit({error_creating,?MODULE,Info})
  end.
  
mk_table(Daddy) ->
  fun() ->
      ets:new(?MODULE,[named_table,ordered_set,public]),
      cfg(timeout,default(timeout)),
      Daddy ! {ok,self()},
      receive quit -> ok end
  end.

find_ts(Tag,Val) ->
  [TS || [TS] <- ets:match(?MODULE,{{Tag,Val,'$1'}})].

%delete all entries that have timed out, i.e. are older than N hours.
prune() ->
  Then = mk_then(cfg(timeout)),
  TSs = ets:select(?MODULE,[{{'$1','_','_'},[{'<','$1',{Then}}],['$1']}]),
  length([delete(TS) || TS <- TSs]).

%return now() minus N hours
mk_then(N) -> 
  mk_then(now(),N*3600).

% handle any TS newer the Beginning of Time, i.e. Jan 1, 1970.
mk_then({MS,S,_},Diff) when Diff=<S -> {MS,S-Diff,0};
mk_then({MS,S,_},Diff) -> mk_then({MS-1,S+1000000,0},Diff).

lookup(TS) ->
  [{TS,I,D}] = ets:lookup(?MODULE,TS),
  I++D.
