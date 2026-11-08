--------------------------- MODULE ParallelSession ---------------------------
(***************************************************************************)
(* The conversation phase of a parallel tree session (Protocol.md §16.8)  *)
(* and its TREE_END barrier (§16.5), at the level of frames.              *)
(*                                                                         *)
(* K connections: 0 is the control connection, 1..K-1 are workers. The    *)
(* model starts after the worker handshake (TREE_WORKER / ACK): every      *)
(* connection is open, and both peers know the expected set Files (triage  *)
(* is deterministic, §16.3).                                               *)
(*                                                                         *)
(* A per-file conversation (§4-§8, several round trips) is abstracted to   *)
(* TREE_FILE + BODY from the sender and ACK or ERR from the receiver. The  *)
(* receiver commits the file (rename) when it sends ACK.                   *)
(*                                                                         *)
(* Faults, at most MaxFaults per behavior: a connection reset, a sender    *)
(* worker thread that dies without raising `stop` (a panic), a local       *)
(* failure inside a conversation on either side.                           *)
(*                                                                         *)
(* Hostile = TRUE replaces the reference sender with one that writes any   *)
(* HostileBudget frames on any connections and closes any of them. The     *)
(* receiver's guards must then keep the safety properties on their own.    *)
(*                                                                         *)
(* The four guard constants are TRUE in the implementation. Setting one to *)
(* FALSE is a mutation, and the checker must find the counterexample       *)
(* (README.md lists each configuration and its expected result).          *)
(*                                                                         *)
(* Code: src/protocol/tree_sync.rs, the parallel branches of tree_send and *)
(* tree_receive. Results: evaluation/verification-pilot.md.                *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
    Files,            \* the expected conversation set
    Extra,            \* a path outside it (only a hostile sender names it)
    NoFile,           \* "no path" in frames that carry none
    K,                \* connection count
    MaxFaults,        \* fault budget per behavior
    Hostile,          \* an arbitrary sender replaces the reference sender
    HostileBudget,    \* frames the hostile sender may write
    BarrierGate,      \* skip the §16.5 barrier read once the control loop
                      \*   consumed TREE_END (`sender_end_consumed`)
    JoinBeforeDecide, \* join the worker threads before deciding the session
    MembershipCheck,  \* TREE_FILE must name a path still in `remaining`
    RemainingCheck    \* success requires `remaining` to be empty

ASSUME /\ K \in 2..4
       /\ Extra \notin Files
       /\ NoFile \notin Files \cup {Extra}
       /\ MaxFaults \in Nat /\ HostileBudget \in Nat
       /\ {Hostile, BarrierGate, JoinBeforeDecide, MembershipCheck,
           RemainingCheck} \subseteq BOOLEAN

Conns   == 0..(K - 1)
Workers == 1..(K - 1)
Paths   == Files \cup {Extra}

Msg(ty, p) == [t |-> ty, f |-> p]
TF(f)     == Msg("TF", f)          \* TREE_FILE
BODY      == Msg("Body", NoFile)   \* the sender's half of a conversation
ACK       == Msg("Ack", NoFile)    \* COMMIT_ACK: the file is committed
ERR       == Msg("Err", NoFile)    \* ERROR, terminal on its connection (§3)
END       == Msg("End", NoFile)    \* the sender's TREE_END barrier
REP       == Msg("Rep", NoFile)    \* the receiver's TREE_END delete report
Msgs      == {TF(f) : f \in Paths} \cup {BODY, ACK, ERR, END, REP}

VARIABLES
    s2r, r2s,      \* [Conns -> Seq(Msgs)]: frames in flight, per direction
    sOpen, rOpen,  \* [Conns -> BOOLEAN]: that peer still holds its end
                   \*   (FALSE: closed; the reader sees EOF after the queue)
    cut,           \* [Conns -> BOOLEAN]: the network reset the connection
    faults,        \* faults used so far
    sPC,           \* sender thread per connection
    pulled,        \* files the shared work queue handed out (the cursor)
    stop,          \* the sender's stop flag
    sErr,          \* [Conns -> BOOLEAN]: that sender thread failed
    sResult,       \* what tree_send returns
    budget,        \* hostile frames left
    rPC,           \* receiver thread per connection
    rCur,          \* the path each receiver thread is serving
    remaining,     \* the expected set not yet announced (membership)
    recorded,      \* first_error is set (worker threads)
    cancel,        \* the receiver's cancel flag
    sessionError,  \* the control thread's own session_error
    sawEnd,        \* the control loop consumed the sender's TREE_END
    committed,     \* [Paths -> Nat]: commits per path
    deleted,       \* the delete pass ran
    rResult        \* what tree_receive returns

net  == <<s2r, r2s, sOpen, rOpen, cut, faults>>
snd  == <<sPC, pulled, stop, sErr, sResult, budget>>
rcv  == <<rPC, rCur, remaining, recorded, cancel, sessionError, sawEnd,
          committed, deleted, rResult>>
vars == <<net, snd, rcv>>

TypeOK ==
    /\ s2r \in [Conns -> Seq(Msgs)] /\ r2s \in [Conns -> Seq(Msgs)]
    /\ sOpen \in [Conns -> BOOLEAN] /\ rOpen \in [Conns -> BOOLEAN]
    /\ cut \in [Conns -> BOOLEAN] /\ faults \in 0..MaxFaults
    /\ sPC \in [Conns -> {"pull", "wait", "join", "endwait", "done"}]
    /\ pulled \subseteq Files /\ stop \in BOOLEAN
    /\ sErr \in [Conns -> BOOLEAN] /\ sResult \in {"none", "ok", "fail"}
    /\ budget \in 0..HostileBudget
    /\ rPC \in [Conns -> {"check", "read", "serving", "join", "barrier",
                          "delete", "done"}]
    /\ rCur \in [Conns -> Paths \cup {NoFile}]
    /\ remaining \subseteq Files
    /\ {recorded, cancel, sessionError, sawEnd, deleted} \subseteq BOOLEAN
    /\ committed \in [Paths -> Nat]
    /\ rResult \in {"none", "ok", "fail"}

Init ==
    /\ s2r = [c \in Conns |-> <<>>]
    /\ r2s = [c \in Conns |-> <<>>]
    /\ sOpen = [c \in Conns |-> TRUE]
    /\ rOpen = [c \in Conns |-> TRUE]
    /\ cut = [c \in Conns |-> FALSE]
    /\ faults = 0
    /\ sPC = [c \in Conns |-> IF Hostile THEN "done" ELSE "pull"]
    /\ pulled = {}
    /\ stop = FALSE
    /\ sErr = [c \in Conns |-> FALSE]
    /\ sResult = "none"
    /\ budget = IF Hostile THEN HostileBudget ELSE 0
    /\ rPC = [c \in Conns |-> "check"]
    /\ rCur = [c \in Conns |-> NoFile]
    /\ remaining = Files
    /\ recorded = FALSE
    /\ cancel = FALSE
    /\ sessionError = FALSE
    /\ sawEnd = FALSE
    /\ committed = [f \in Paths |-> 0]
    /\ deleted = FALSE
    /\ rResult = "none"

(* ---- Network --------------------------------------------------------- *)

\* A write to a reset connection goes nowhere.
Put(q, c, m) == IF cut[c] THEN q ELSE [q EXCEPT ![c] = Append(@, m)]
Pop(q, c)    == [q EXCEPT ![c] = Tail(@)]

\* What a blocking read sees. A reset is an error even with frames queued
\* (Cut drops them); EOF comes only after every queued frame was read.
RHas(c) == ~cut[c] /\ s2r[c] # <<>>
REof(c) == ~cut[c] /\ s2r[c] = <<>> /\ ~sOpen[c]
SHas(c) == ~cut[c] /\ r2s[c] # <<>>
SEof(c) == ~cut[c] /\ r2s[c] = <<>> /\ ~rOpen[c]

Cut(c) ==
    /\ faults < MaxFaults /\ ~cut[c] /\ (sOpen[c] \/ rOpen[c])
    /\ cut' = [cut EXCEPT ![c] = TRUE]
    /\ s2r' = [s2r EXCEPT ![c] = <<>>]
    /\ r2s' = [r2s EXCEPT ![c] = <<>>]
    /\ faults' = faults + 1
    /\ UNCHANGED <<sOpen, rOpen, snd, rcv>>

(* ---- Reference sender (tree_send, parallel branch) -------------------- *)

\* A sender thread leaves its loop. A worker returns, which drops and so
\* closes its connection; the control thread goes on to join the workers.
SLeave(c) ==
    IF c = 0 THEN /\ sPC' = [sPC EXCEPT ![0] = "join"]
                  /\ UNCHANGED sOpen
             ELSE /\ sPC' = [sPC EXCEPT ![c] = "done"]
                  /\ sOpen' = [sOpen EXCEPT ![c] = FALSE]

\* next_job, then TREE_FILE and the conversation. Any order over the queue
\* (the reference hands out sorted order; the receiver must not care).
\* Checking `stop` atomically with the pull is faithful: a store to `stop`
\* between next_job's load and its fetch_add has the same effect as one
\* right after the fetch_add.
SPull(c) ==
    /\ sPC[c] = "pull" /\ ~stop
    /\ \E f \in Files \ pulled :
          /\ pulled' = pulled \cup {f}
          /\ s2r' = Put(Put(s2r, c, TF(f)), c, BODY)
    /\ sPC' = [sPC EXCEPT ![c] = "wait"]
    /\ UNCHANGED <<r2s, sOpen, rOpen, cut, faults, stop, sErr, sResult,
                   budget, rcv>>

\* A local failure inside the conversation (the file vanished, a read
\* error): ERROR on that connection, raise `stop`.
SPullFail(c) ==
    /\ sPC[c] = "pull" /\ ~stop /\ faults < MaxFaults
    /\ \E f \in Files \ pulled :
          /\ pulled' = pulled \cup {f}
          /\ s2r' = Put(Put(s2r, c, TF(f)), c, ERR)
    /\ faults' = faults + 1
    /\ stop' = TRUE
    /\ sErr' = [sErr EXCEPT ![c] = TRUE]
    /\ SLeave(c)
    /\ UNCHANGED <<r2s, rOpen, cut, sResult, budget, rcv>>

\* The queue is drained, or another thread failed.
SNoJob(c) ==
    /\ sPC[c] = "pull" /\ (stop \/ pulled = Files)
    /\ SLeave(c)
    /\ UNCHANGED <<s2r, r2s, rOpen, cut, faults, pulled, stop, sErr, sResult,
                   budget, rcv>>

SAck(c) ==
    /\ sPC[c] = "wait" /\ SHas(c) /\ Head(r2s[c]) = ACK
    /\ r2s' = Pop(r2s, c)
    /\ sPC' = [sPC EXCEPT ![c] = "pull"]
    /\ UNCHANGED <<s2r, sOpen, rOpen, cut, faults, pulled, stop, sErr,
                   sResult, budget, rcv>>

\* The receiver's ERROR, EOF, or a reset: the conversation failed. The
\* thread raises `stop` and sends ERROR (the control thread's
\* announce_abort skips peer errors; the receiver reads neither again).
SFail(c) ==
    /\ sPC[c] = "wait"
    /\ \/ SHas(c) /\ Head(r2s[c]) # ACK
       \/ SEof(c)
       \/ cut[c]
    /\ r2s' = IF SHas(c) THEN Pop(r2s, c) ELSE r2s
    /\ s2r' = Put(s2r, c, ERR)
    /\ stop' = TRUE
    /\ sErr' = [sErr EXCEPT ![c] = TRUE]
    /\ SLeave(c)
    /\ UNCHANGED <<rOpen, cut, faults, pulled, sResult, budget, rcv>>

\* A worker thread dies without raising `stop` (a panic): its connection
\* closes; the join reports it.
SCrash(w) ==
    /\ sPC[w] \in {"pull", "wait"} /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ sErr' = [sErr EXCEPT ![w] = TRUE]
    /\ sPC' = [sPC EXCEPT ![w] = "done"]
    /\ sOpen' = [sOpen EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<s2r, r2s, rOpen, cut, pulled, stop, sResult, budget, rcv>>

\* Join the workers. Any error: return it, which closes the control
\* connection WITHOUT an ERROR frame (as implemented). Otherwise TREE_END.
SJoin ==
    /\ sPC[0] = "join" /\ \A w \in Workers : sPC[w] = "done"
    /\ IF \E c \in Conns : sErr[c]
          THEN /\ sPC' = [sPC EXCEPT ![0] = "done"]
               /\ sOpen' = [sOpen EXCEPT ![0] = FALSE]
               /\ sResult' = "fail"
               /\ UNCHANGED s2r
          ELSE /\ sPC' = [sPC EXCEPT ![0] = "endwait"]
               /\ s2r' = Put(s2r, 0, END)
               /\ UNCHANGED <<sOpen, sResult>>
    /\ UNCHANGED <<r2s, rOpen, cut, faults, pulled, stop, sErr, budget, rcv>>

\* Read the delete report.
SEnd ==
    /\ sPC[0] = "endwait" /\ (SHas(0) \/ SEof(0) \/ cut[0])
    /\ sResult' = IF SHas(0) /\ Head(r2s[0]) = REP THEN "ok" ELSE "fail"
    /\ r2s' = IF SHas(0) THEN Pop(r2s, 0) ELSE r2s
    /\ sPC' = [sPC EXCEPT ![0] = "done"]
    /\ sOpen' = [sOpen EXCEPT ![0] = FALSE]
    /\ UNCHANGED <<s2r, rOpen, cut, faults, pulled, stop, sErr, budget, rcv>>

(* ---- Hostile sender ----------------------------------------------------- *)

HSend ==
    /\ Hostile /\ budget > 0
    /\ \E c \in Conns, m \in {TF(f) : f \in Paths} \cup {BODY, ERR, END} :
          /\ sOpen[c]
          /\ s2r' = Put(s2r, c, m)
    /\ budget' = budget - 1
    /\ UNCHANGED <<r2s, sOpen, rOpen, cut, faults, sPC, pulled, stop, sErr,
                   sResult, rcv>>

HClose ==
    /\ Hostile
    /\ \E c \in Conns : sOpen[c] /\ sOpen' = [sOpen EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<s2r, r2s, rOpen, cut, faults, snd, rcv>>

(* ---- Receiver (tree_receive, parallel branch) -------------------------- *)

\* A service loop ends. A worker thread returns, dropping its stream; the
\* control thread goes on to join the workers.
RLeave(c) ==
    IF c = 0 THEN /\ rPC' = [rPC EXCEPT ![0] = "join"]
                  /\ UNCHANGED rOpen
             ELSE /\ rPC' = [rPC EXCEPT ![c] = "done"]
                  /\ rOpen' = [rOpen EXCEPT ![c] = FALSE]

\* Record a failure: a worker through first_error + cancel, the control
\* thread through its own session_error.
RFailure(c) ==
    IF c = 0 THEN /\ sessionError' = TRUE
                  /\ UNCHANGED <<recorded, cancel>>
             ELSE /\ recorded' = TRUE /\ cancel' = TRUE
                  /\ UNCHANGED sessionError

\* `cancel` is checked before each read, never during one: a thread already
\* blocked in a read serves whatever arrives next.
RCheck(c) ==
    /\ rPC[c] = "check"
    /\ IF cancel THEN RLeave(c)
                 ELSE /\ rPC' = [rPC EXCEPT ![c] = "read"]
                      /\ UNCHANGED rOpen
    /\ UNCHANGED <<s2r, r2s, sOpen, cut, faults, snd, rCur, remaining,
                   recorded, cancel, sessionError, sawEnd, committed, deleted,
                   rResult>>

\* TREE_FILE naming an expected path: take it from `remaining`, serve it.
RTake(c) ==
    /\ rPC[c] = "read" /\ RHas(c) /\ Head(s2r[c]).t = "TF"
    /\ LET f == Head(s2r[c]).f IN
          /\ f \in remaining \/ ~MembershipCheck
          /\ remaining' = remaining \ {f}
          /\ rCur' = [rCur EXCEPT ![c] = f]
    /\ s2r' = Pop(s2r, c)
    /\ rPC' = [rPC EXCEPT ![c] = "serving"]
    /\ UNCHANGED <<r2s, sOpen, rOpen, cut, faults, snd, recorded, cancel,
                   sessionError, sawEnd, committed, deleted, rResult>>

\* TREE_FILE outside the set, or already taken. A worker answers ERROR;
\* the control loop breaks without one (as implemented).
RReject(c) ==
    /\ rPC[c] = "read" /\ RHas(c) /\ Head(s2r[c]).t = "TF"
    /\ MembershipCheck /\ Head(s2r[c]).f \notin remaining
    /\ s2r' = Pop(s2r, c)
    /\ r2s' = IF c = 0 THEN r2s ELSE Put(r2s, c, ERR)
    /\ RFailure(c) /\ RLeave(c)
    /\ UNCHANGED <<sOpen, cut, faults, snd, rCur, remaining, sawEnd,
                   committed, deleted, rResult>>

\* The sender's TREE_END on the control connection.
REnd ==
    /\ rPC[0] = "read" /\ RHas(0) /\ Head(s2r[0]) = END
    /\ s2r' = Pop(s2r, 0)
    /\ sawEnd' = TRUE
    /\ rPC' = [rPC EXCEPT ![0] = "join"]
    /\ UNCHANGED <<r2s, sOpen, rOpen, cut, faults, snd, rCur, remaining,
                   recorded, cancel, sessionError, committed, deleted,
                   rResult>>

\* ERROR, BODY, or (on a worker) TREE_END where a TREE_FILE belongs.
RBadFrame(c) ==
    /\ rPC[c] = "read" /\ RHas(c) /\ Head(s2r[c]).t # "TF"
    /\ ~(c = 0 /\ Head(s2r[c]) = END)
    /\ s2r' = Pop(s2r, c)
    /\ RFailure(c) /\ RLeave(c)
    /\ UNCHANGED <<r2s, sOpen, cut, faults, snd, rCur, remaining, sawEnd,
                   committed, deleted, rResult>>

\* EOF between conversations: the sender has nothing more for this
\* connection. Not an error by itself; the decision checks the rest.
REofIdle(c) ==
    /\ rPC[c] = "read" /\ REof(c)
    /\ RLeave(c)
    /\ UNCHANGED <<s2r, r2s, sOpen, cut, faults, snd, rCur, remaining,
                   recorded, cancel, sessionError, sawEnd, committed, deleted,
                   rResult>>

RResetIdle(c) ==
    /\ rPC[c] = "read" /\ cut[c]
    /\ RFailure(c) /\ RLeave(c)
    /\ UNCHANGED <<s2r, r2s, sOpen, cut, faults, snd, rCur, remaining, sawEnd,
                   committed, deleted, rResult>>

\* The conversation succeeds: commit (rename), then COMMIT_ACK.
RCommit(c) ==
    /\ rPC[c] = "serving" /\ RHas(c) /\ Head(s2r[c]) = BODY
    /\ s2r' = Pop(s2r, c)
    /\ committed' = [committed EXCEPT ![rCur[c]] = @ + 1]
    /\ r2s' = Put(r2s, c, ACK)
    /\ rPC' = [rPC EXCEPT ![c] = "check"]
    /\ UNCHANGED <<sOpen, rOpen, cut, faults, snd, rCur, remaining, recorded,
                   cancel, sessionError, sawEnd, deleted, rResult>>

\* A conversation fails: answer ERROR, record it, leave.
RFailConv(c) ==
    /\ s2r' = IF RHas(c) THEN Pop(s2r, c) ELSE s2r
    /\ r2s' = Put(r2s, c, ERR)
    /\ RFailure(c) /\ RLeave(c)
    /\ UNCHANGED <<sOpen, cut, snd, rCur, remaining, sawEnd, committed,
                   deleted, rResult>>

\* ... because the sender sent ERROR or a wrong frame, closed, or reset.
RConvFail(c) ==
    /\ rPC[c] = "serving"
    /\ \/ RHas(c) /\ Head(s2r[c]) # BODY
       \/ REof(c) \/ cut[c]
    /\ UNCHANGED faults
    /\ RFailConv(c)

\* ... because of a local fault (verify mismatch, disk full).
RConvFault(c) ==
    /\ rPC[c] = "serving" /\ RHas(c) /\ Head(s2r[c]) = BODY
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ RFailConv(c)

\* Join the workers, then decide: any recorded error, a missing TREE_END,
\* or an expected path never announced fails the session.
RDecide ==
    /\ rPC[0] = "join"
    /\ JoinBeforeDecide => \A w \in Workers : rPC[w] = "done"
    /\ IF \/ sessionError \/ recorded \/ ~sawEnd
          \/ RemainingCheck /\ remaining # {}
          THEN /\ rResult' = "fail"
               /\ rPC' = [rPC EXCEPT ![0] = "done"]
               /\ rOpen' = [rOpen EXCEPT ![0] = FALSE]
          ELSE /\ rPC' = [rPC EXCEPT ![0] =
                            IF BarrierGate THEN "delete" ELSE "barrier"]
               /\ UNCHANGED <<rResult, rOpen>>
    /\ UNCHANGED <<s2r, r2s, sOpen, cut, faults, snd, rCur, remaining,
                   recorded, cancel, sessionError, sawEnd, committed, deleted>>

\* Only without the gate: the sequential §16.5 barrier read, although the
\* control loop already consumed the TREE_END.
RBarrier ==
    /\ rPC[0] = "barrier" /\ (RHas(0) \/ REof(0) \/ cut[0])
    /\ IF RHas(0) /\ Head(s2r[0]) = END
          THEN /\ rPC' = [rPC EXCEPT ![0] = "delete"]
               /\ UNCHANGED <<rResult, rOpen>>
          ELSE /\ rPC' = [rPC EXCEPT ![0] = "done"]
               /\ rResult' = "fail"
               /\ rOpen' = [rOpen EXCEPT ![0] = FALSE]
    /\ s2r' = IF RHas(0) THEN Pop(s2r, 0) ELSE s2r
    /\ UNCHANGED <<r2s, sOpen, cut, faults, snd, rCur, remaining, recorded,
                   cancel, sessionError, sawEnd, committed, deleted>>

\* The delete pass, then the TREE_END report.
RDelete ==
    /\ rPC[0] = "delete"
    /\ deleted' = TRUE
    /\ r2s' = Put(r2s, 0, REP)
    /\ rResult' = "ok"
    /\ rPC' = [rPC EXCEPT ![0] = "done"]
    /\ rOpen' = [rOpen EXCEPT ![0] = FALSE]
    /\ UNCHANGED <<s2r, sOpen, cut, faults, snd, rCur, remaining, recorded,
                   cancel, sessionError, sawEnd, committed>>

(* ---- Specification ------------------------------------------------------ *)

AllDone == /\ \A c \in Conns : rPC[c] = "done"
           /\ Hostile \/ \A c \in Conns : sPC[c] = "done"

\* Faults may happen; they never have to. Fairness covers progress only, so
\* a hang that only a fault (a reset: the socket timeout) would break
\* violates Termination instead of hiding behind the fault.
Fault ==
    \/ \E c \in Conns : Cut(c) \/ SPullFail(c) \/ RConvFault(c)
    \/ \E w \in Workers : SCrash(w)

Progress ==
    \/ \E c \in Conns :
          \/ SPull(c) \/ SNoJob(c) \/ SAck(c) \/ SFail(c)
          \/ RCheck(c) \/ RTake(c) \/ RReject(c) \/ RBadFrame(c)
          \/ REofIdle(c) \/ RResetIdle(c) \/ RCommit(c) \/ RConvFail(c)
    \/ SJoin \/ SEnd \/ HSend \/ HClose
    \/ REnd \/ RDecide \/ RBarrier \/ RDelete

\* Stuttering at the end, so a finished session is not a deadlock. A
\* hostile sender may go silent at any point; a receiver blocked on it is
\* the socket timeout's business, so hostile runs check safety only.
Finished    == AllDone /\ UNCHANGED vars
HostileIdle == Hostile /\ UNCHANGED vars

Next == Progress \/ Fault \/ Finished \/ HostileIdle

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

(* ---- Properties --------------------------------------------------------- *)

\* Membership: every expected path is committed at most once, and nothing
\* outside the set is ever committed.
ExactlyOnce == \A f \in Files : committed[f] <= 1
NoStranger  == committed[Extra] = 0

\* Deletes only after every expected conversation committed.
DeleteSafe == deleted => \A f \in Files : committed[f] = 1

\* The receiver starts deleting only when it has seen no failure.
DecideClean ==
    [][~deleted /\ deleted' => ~recorded /\ ~sessionError]_vars

\* Reference sender only. A failure on the sender side never ends in
\* deletes, and the sender reports success only once the receiver deleted.
FailClosed  == deleted => \A c \in Conns : ~sErr[c]
SenderTruth == sResult = "ok" => deleted /\ \A f \in Files : committed[f] = 1

\* Reference sender only: every behavior ends with all threads done, so no
\* hang has to be broken by a socket timeout.
Termination == <>AllDone

(* ---- Witnesses ---------------------------------------------------------- *)
(* Each is EXPECTED to be violated (Witness.cfg): the trace proves the     *)
(* model reaches that outcome, so the properties above are not vacuous.    *)

\* A full success: the receiver deleted, the sender read the report.
NoSuccess == sResult # "ok"

\* A failure the receiver detected through the decision, not a frame.
NoQuietFailure == ~(rResult = "fail" /\ ~recorded /\ ~sessionError)

\* The split outcome no protocol can rule out: a reset after the report
\* left the receiver loses it, the receiver succeeded, the sender fails.
NoSplitOutcome == ~(rResult = "ok" /\ sResult = "fail")
=============================================================================
