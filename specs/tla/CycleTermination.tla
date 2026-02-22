---- MODULE CycleTermination ----
\* Formal model of the cycle termination protocol from:
\*   pkg/server/commands/reverseexpand/pipeline/resolver_base.go (lines 129-141)
\*   pkg/server/commands/reverseexpand/pipeline/cycle.go
\*   pkg/server/commands/reverseexpand/pipeline/track/reporting.go
\*
\* Three-phase shutdown:
\*   1. wgStandard.Wait() + SignalReady()  -> pool[w] = false
\*   2. WaitForAllReady()                  -> forall v: pool[v] = false
\*   3. WaitForDrain()                     -> tracker < 1
\*   4. Close listener pipes
\*
\* Cyclical goroutines (wgRecursive) run concurrently throughout phases 1-4,
\* receiving messages (tracker.Dec) and sending new ones (tracker.Inc per listener).
\* Input deduplication (cyclicWork) bounds the total new messages they can generate.

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Workers,       \* Set of workers sharing a cycle, e.g. {"w1", "w2"}
    MaxCyclicWork  \* Bound on unique values processable by cyclical goroutines

ASSUME
    /\ Workers # {}
    /\ MaxCyclicWork \in Nat

\* -----------------------------------------------------------------------
\* State variables
\* -----------------------------------------------------------------------
VARIABLES
    tracker,     \* Shared Tracker.value: in-flight message count (always >= 0)
    pool,        \* StatusPool: pool[w] = TRUE means worker w is "busy"
    phase,       \* Termination phase for each worker
    cyclicWork   \* Remaining unique values cyclical goroutines can process

vars == <<tracker, pool, phase, cyclicWork>>

Phases == {"Active", "StandardDone", "AllReady", "ClosingPipes", "Done"}

TypeOK ==
    /\ tracker    \in Nat
    /\ pool       \in [Workers -> BOOLEAN]
    /\ phase      \in [Workers -> Phases]
    /\ cyclicWork \in 0..MaxCyclicWork

\* -----------------------------------------------------------------------
\* Initial state
\* -----------------------------------------------------------------------
\* Seed message from listenForInitialValue pre-increments tracker once.
\* All workers start busy (pool[w] = TRUE), as set in cycleGroup.Join().
Init ==
    /\ tracker    = 1
    /\ pool       = [w \in Workers |-> TRUE]
    /\ phase      = [w \in Workers |-> "Active"]
    /\ cyclicWork = MaxCyclicWork

\* -----------------------------------------------------------------------
\* Actions
\* -----------------------------------------------------------------------

\* wgStandard.Wait() completed; worker calls SignalReady() (reporter.Report(false)).
\* Corresponds to resolver_base.go lines 129-132.
StandardDone(w) ==
    /\ phase[w] = "Active"
    /\ phase' = [phase EXCEPT ![w] = "StandardDone"]
    /\ pool'  = [pool  EXCEPT ![w] = FALSE]
    /\ UNCHANGED <<tracker, cyclicWork>>

\* Cyclical goroutine receives a message and processes it (drain loop in resolver_core.go).
\* May generate new messages if the value is unique (cyclicWork > 0).
\* Net tracker change:
\*   unique value -> tracker += (open listeners - 1); open = non-Done workers
\*   duplicate   -> tracker -= 1
\* Fires for any worker whose pipe is not yet closed (phase /= Done).
CyclicProcess(w) ==
    /\ tracker > 0
    /\ phase[w] # "Done"
    /\ LET openCount == Cardinality({v \in Workers : phase[v] # "Done"})
       IN
       \/ \* Unique value: input dedup misses; generates messages to open listeners
          /\ cyclicWork > 0
          /\ tracker'    = tracker + (openCount - 1)
          /\ cyclicWork' = cyclicWork - 1
       \/ \* Duplicate value: input dedup fires; no output
          /\ tracker'    = tracker - 1
          /\ UNCHANGED cyclicWork
    /\ UNCHANGED <<pool, phase>>

\* WaitForAllReady() unblocks: all workers have reported idle.
\* Corresponds to resolver_base.go line 133 / reporting.go StatusPool.Wait.
AllReady(w) ==
    /\ phase[w] = "StandardDone"
    /\ \A v \in Workers : pool[v] = FALSE   \* StatusPool.get() = false
    /\ phase' = [phase EXCEPT ![w] = "AllReady"]
    /\ UNCHANGED <<tracker, pool, cyclicWork>>

\* WaitForDrain() unblocks: tracker has reached zero.
\* Corresponds to resolver_base.go line 134 / track/reporting.go Tracker.Wait.
Drain(w) ==
    /\ phase[w] = "AllReady"
    /\ tracker = 0                           \* tracker < 1
    /\ phase' = [phase EXCEPT ![w] = "ClosingPipes"]
    /\ UNCHANGED <<tracker, pool, cyclicWork>>

\* Worker closes its listener pipes; cyclical goroutines (wgRecursive) drain and exit.
\* Corresponds to resolver_base.go lines 137-141.
ClosePipes(w) ==
    /\ phase[w] = "ClosingPipes"
    /\ phase' = [phase EXCEPT ![w] = "Done"]
    /\ UNCHANGED <<tracker, pool, cyclicWork>>

\* Terminal stuttering action: once all workers are Done the system stays Done.
\* Required to prevent TLC from flagging the intended terminal state as a deadlock.
Terminating ==
    /\ \A w \in Workers : phase[w] = "Done"
    /\ UNCHANGED vars

Next ==
    \/ \E w \in Workers :
        \/ StandardDone(w)
        \/ CyclicProcess(w)
        \/ AllReady(w)
        \/ Drain(w)
        \/ ClosePipes(w)
    \/ Terminating

\* Weak fairness: no worker stalls indefinitely in any protocol phase.
\* Strong fairness on CyclicProcess: cyclical goroutines always eventually drain.
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ \A w \in Workers :
        /\ WF_vars(StandardDone(w))
        /\ WF_vars(AllReady(w))
        /\ WF_vars(Drain(w))
        /\ WF_vars(ClosePipes(w))
    /\ SF_vars(\E w \in Workers : CyclicProcess(w))

\* -----------------------------------------------------------------------
\* Safety properties (invariants)
\* -----------------------------------------------------------------------

\* S1: tracker is always non-negative.
TrackerNonNegative == tracker >= 0

\* S2: A worker only enters ClosingPipes/Done when tracker == 0.
\*     Guaranteed by Drain(w)'s guard; verified by TLC.
SafeClose ==
    \A w \in Workers :
        phase[w] \in {"ClosingPipes", "Done"} => tracker = 0

\* S3: No worker closes pipes before every worker has called SignalReady.
TerminationOrder ==
    \A w \in Workers :
        phase[w] \in {"ClosingPipes", "Done"} =>
        \A v \in Workers : phase[v] # "Active"

\* -----------------------------------------------------------------------
\* Liveness property (temporal)
\* -----------------------------------------------------------------------

\* L1: All workers eventually complete.
EventualTermination ==
    <> (\A w \in Workers : phase[w] = "Done")

====
