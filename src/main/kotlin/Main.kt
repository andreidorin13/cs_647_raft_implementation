import akka.actor.*
import java.time.Duration
import kotlin.random.Random
import java.io.*
import java.lang.Exception

// ----- Raft Agent -----  
enum class Role {
    Follower, Candidate, Leader
}

const val HeartbeatInterval = 300L
const val StepdownInterval = 10 * HeartbeatInterval
val TermTimeoutRange = Pair(2000L, 5000L)
val ElectionTimeoutRange = Pair(1000L, 2000L)

class RaftAgent<T>: AbstractActorWithTimers() {
    private val id = self.path().name().toInt()
    private var majority: Int = 0 // to be overwritten on init
    private lateinit var peers: List<ActorRef>

    private val p = PersistentStorage("$id.persist")
    private val s = StateMachine<T>("$id.state")
    private val l = Log<T>("$id.log")

    // State Vars
    private var leaderId = -1
    // Candidate state
    private var termVotes = 0
    // Leader vars
    private var sequenceNum = 0
    private val responses = HashMap<Int, MutableSet<Int>>()

    // Mapping transition functions to Roles
    private lateinit var role: Role
    private val from = mapOf(
        Role.Follower to this::fromFollower,
        Role.Candidate to this::fromCandidate,
        Role.Leader to this::fromLeader
    )
    private val to = mapOf(
        Role.Follower to this::toFollower,
        Role.Candidate to this::toCandidate,
        Role.Leader to this::toLeader
    )

    private val leader = receiveBuilder()
        .match(Heartbeat::class.java) {
            heartbeat()
        }
        .match(StepDown::class.java) {
            fromLeader { toFollower()}
        }
        .match(RaftRequest::class.java) { req->
            println("$id: Received ${req.type} Command key: ${req.cmd.key}")
            if (req.type == RaftRequestType.Get) {
                sender.tell(RaftResponse(leaderId, s[req.cmd.key]), self)
                return@match
            } // else it's a set request, reply with leader id so client can double check
            // it's talking to the right node
            sender.tell(RaftResponse(leaderId, null), self)

            // Haven't figured out a good way to match on a RaftRequest<T>
            // only RaftRequest<*> so casts are necessary
            @Suppress("UNCHECKED_CAST")
            l.add(LogEntry(p.currentTerm, req.cmd as Command<T>))
            appendEntries()
        }
        .match(AppendResponse::class.java) { msg->
            println("$id: Got Response: Term: ${msg.term}, Success: ${msg.success}, LastLog: ${msg.lastLogIndex}, Id: ${msg.requestId} from ${sender.path().name()}")
            validTerm(msg, sender) {
                commitCheck()

                val senderId = sender.path().name().toInt()
                if (msg.requestId in responses) {
                    val set = responses[msg.requestId]!!
                    set.add(senderId)

                    if (set.size + 1 > majority) {
                        // Received majority heartbeats in interval, no need to step down
                        tikTok(Timers.StepDown, StepDown(), StepdownInterval)
                        responses.remove(msg.requestId)
                    }
                }

                if (!msg.success) {
                    l.nextIndex[senderId] = Math.max(l.nextIndex[senderId] - 1, 1)
                } else {
                    l.nextIndex[senderId] = msg.lastLogIndex + 1
                    l.matchIndex[senderId] = msg.lastLogIndex

                    var majorityCommitIndex = 0
                    for (i in l.commitIndex + 1 until l.lastIndex + 1) {
                        val count = l.matchIndex.filter { it >= i }.size
                        if (l[i].term == p.currentTerm && count + 1 > majority)
                            majorityCommitIndex = i
                        else
                            break
                    }
                    if (majorityCommitIndex > l.commitIndex) {
                        l.commitIndex = majorityCommitIndex
                    }
                }
                if (l.lastIndex >= l.nextIndex[senderId])
                    appendEntries(sender) // Need to catch up the sender
            }
        }
        .build()

    private fun heartbeat() {
        println("$id: Heartbeating!")
        sequenceNum++
        responses[sequenceNum] = mutableSetOf()
        appendEntries()
    }

    private fun appendEntries(to: ActorRef? = null) {
        val recipients = if (to != null) listOf(to) else peers
        for (rec in recipients) {
            val next = l.nextIndex[rec.path().name().toInt()]
            val prev = next - 1
            val entry = if (l.lastIndex >= next) l[next] else null

            val msg = AppendEntries(
                seqNum = sequenceNum,
                term = p.currentTerm,
                leaderId = id,
                leaderCommitIndex = l.commitIndex,
                prevLogIndex = prev,
                prevLogTerm = if (prev > 0) l[prev].term else 0,
                entry = entry
            )
            println("$id: Sending append entry: $msg to ${rec.path().name()}")
            rec.tell(msg, self)
        }
    }

    private val candidate = receiveBuilder()
        .match(ElectionTimeout::class.java) {
            println("$id: Election timed out")
            fromCandidate { toFollower() }
        }
        .match(RaftRequest::class.java) {
            println("$id: Got command, no leader, sending -1 to wait")
            sender.tell(RaftResponse(-1, null), self)
        }
        .match(VoteResponse::class.java) { msg->
            validTerm(msg, sender) {
                if (msg.success) {
                    termVotes++
                    if (termVotes > majority)
                        fromCandidate { toLeader() }
                }
            }
        }
        .match(AppendEntries::class.java) { msg->
            validTerm(msg, sender) {
                println("$id: Got append entry going to follower!")
                if (p.currentTerm == msg.term) // found new leader for term
                    fromCandidate { toFollower() }
            }
        }
        .build()

    private val follower = receiveBuilder()
        .match(ElectionTime::class.java) {
            println("$id: Election timed out, upgrading to Candidate")
            fromFollower { toCandidate() }
        }
        .match(RaftRequest::class.java) {
            println("$id: Got command, rerouting to leader $leaderId")
            sender.tell(RaftResponse(leaderId, null), self)
        }
        .match(RequestVote::class.java) { msg ->
            println("$id: Received vote request: $msg")
            validTerm(msg, sender) {
                val logAhead =
                    msg.lastLogTerm > l.lastTerm ||
                            (msg.lastLogTerm == l.lastTerm && msg.lastLogIndex >= l.lastIndex)

                val success = p.votedFor == -1 && logAhead
                if (success)
                    p.votedFor = msg.candidateId
                println("$id: Voted $success for ${msg.candidateId}")
                sender.tell(VoteResponse(p.currentTerm, success), self)
            }
        }
        .match(AppendEntries::class.java) { msg->
            validTerm(msg, sender) {
                commitCheck()
                leaderId = msg.leaderId

                try {
                    if (msg.prevLogIndex > l.lastIndex || l[msg.prevLogIndex].term != msg.prevLogTerm) {
                        sender.tell(AppendResponse(p.currentTerm, false, msg.seqNum, l.lastIndex), self)
                        return@validTerm
                    }
                } catch (e: Exception) { }

                val newIndex = msg.prevLogIndex + 1
                try {
                    if (l[newIndex].term != msg.term || (l.lastIndex != msg.prevLogIndex))
                        l.deleteSince(newIndex)
                } catch (e: Exception) {}

                if (msg.entry != null) {
                    @Suppress("UNCHECKED_CAST")
                    l.add(msg.entry as LogEntry<T>)
                    println("$id: Got entry: ${msg.entry.cmd}")
                }

                if (l.commitIndex < msg.leaderCommitIndex)
                    l.commitIndex = Math.min(msg.leaderCommitIndex, l.lastIndex)

                sender.tell(AppendResponse(p.currentTerm, true, msg.seqNum, l.lastIndex), self)
                tikTok(Timers.Election, ElectionTime(), Random.nextLong(TermTimeoutRange.first, TermTimeoutRange.second))
            }
        }
        .build()

    override fun createReceive(): Receive =
        receiveBuilder()
            .match(Init::class.java) { msg ->
                peers = msg.agents.filter { it.path().name().toInt() != id }
                majority = (peers.size + 1) / 2
                toFollower()
            }
            .build()

    private fun validTerm(msg: Msg, sender: ActorRef, next: ()->Unit) {
        if (p.currentTerm < msg.term) {
            p.currentTerm = msg.term
            p.votedFor = -1
            println("$id: Got newer term, updating to ${msg.term}")
            if (role != Role.Follower) {
                from[role]?.invoke{to[Role.Follower]}
            }
        }
        if (p.currentTerm > msg.term && msg !is Response) { // No reason to reply to Responses
            when (msg) {
                is RequestVote -> sender.tell(VoteResponse(p.currentTerm, false), self)
                is AppendEntries<*> -> sender.tell(AppendResponse(p.currentTerm, false, msg.seqNum), self)
            }
            return
        }
        next()
    }

    private fun commitCheck() {
        for (todo in l.lastApplied + 1 until l.commitIndex + 1) {
            s.apply(l[todo].cmd)
            l.lastApplied++
        }
    }

    private fun tikTok(key: Timers, msg: Any, time: Long) {
        timers.cancel(key)
        timers.startPeriodicTimer(key, msg, Duration.ofMillis(time))
    }

    private fun fromFollower(to: ()->Unit) {
        timers.cancel(Timers.Election)
        to()
    }
    private fun fromCandidate(to: ()->Unit) {
        timers.cancel(Timers.ElectionTimeout)
        to()
    }
    private fun fromLeader(to: ()->Unit) {
        timers.cancel(Timers.Heartbeat)
        timers.cancel(Timers.StepDown)
        to()
    }
    private fun toFollower() {
        println("$id: Reverting to Follower")
        context.become(follower)
        role = Role.Follower
        p.votedFor = -1
        leaderId = -1
        tikTok(Timers.Election, ElectionTime(), Random.nextLong(TermTimeoutRange.first, TermTimeoutRange.second)) // 1-2s
    }
    private fun toCandidate() {
        context.become(candidate)
        role = Role.Candidate
        leaderId = -1 // forget leader
        p.currentTerm++
        p.votedFor = id
        termVotes = 1 // vote for self
        // Request vote
        println("$id: Sending vote request")
        peers.map { it.tell(RequestVote(p.currentTerm, id, l.lastIndex, l.lastTerm), self) }
        tikTok(Timers.ElectionTimeout, ElectionTimeout(), Random.nextLong(ElectionTimeoutRange.first, ElectionTimeoutRange.second))
    }
    private fun toLeader() {
        println("$id: Escalating to leader!")
        context.become(leader)
        role = Role.Leader
        leaderId = id
        sequenceNum = 0
        responses.clear()

        l.nextIndex = MutableList(peers.size + 1) { l.lastIndex + 1 }
        l.matchIndex = MutableList(peers.size + 1) { 0 }

        heartbeat()
        tikTok(Timers.Heartbeat, Heartbeat(), HeartbeatInterval)
        tikTok(Timers.StepDown, StepDown(), StepdownInterval)
    }
}

// ----- Raft Data Structures -----  
// Client Interaction
enum class RaftRequestType {
    Get, Set
}

class RaftRequest<T>(val type: RaftRequestType, val cmd: Command<T>)
class RaftResponse<T>(val leader: Int, val value: T)

class Command <T>(val key: String, val value: T): Serializable

// Persistent backend that flushes to disk
open class KeyValueStore<T>(private val filename: String) {
    operator fun get(k: String): T? = load()[k]

    operator fun set(k: String, v: T) {
        val tmp = load()
        tmp[k] = v
        save(tmp)
    }

    private fun load(): MutableMap<String, T> {
        try {
            ObjectInputStream(FileInputStream(filename)).use {
                @Suppress("UNCHECKED_CAST")
                return it.readObject() as MutableMap<String, T>
            }
        } catch (e: Exception) {
            return mutableMapOf()
        }
    }

    private fun save(s: MutableMap<String, T>) {
        ObjectOutputStream(FileOutputStream(filename)).use { it.writeObject(s) }
    }
}

class StateMachine<T>(filename: String): KeyValueStore<T>(filename) {
    fun apply(cmd: Command<T>) {
        this[cmd.key] = cmd.value
    }
}

class PersistentStorage(filename: String) {
    private val store = KeyValueStore<Int>(filename)
    private val termKey = "currentTerm"
    private val voteKey = "votedFor"

    var currentTerm: Int
        get() = store[termKey] ?: 0
        set(value) { store[termKey] = value }

    var votedFor: Int
        get() = store[voteKey] ?: -1
        set(value) { store[voteKey] = value }
}

// Log Section
data class LogEntry<T>(val term: Int, val cmd: Command<T>): Serializable

class Log<T>(private val filename: String) {
    private var entries: MutableList<LogEntry<T>> = read()
    // Volatile state
    var commitIndex = 0
    var lastApplied = 0
    // Leaders only
    var matchIndex: MutableList<Int> = mutableListOf()
    var nextIndex: MutableList<Int> = mutableListOf()

    val lastTerm: Int
        get() = entries.lastOrNull()?.term ?: 0

    val lastIndex: Int
        get() = entries.size

    operator fun get(index: Int) = entries[index - 1] // Term lookup

    @Synchronized fun add(logEntry: LogEntry<T>) {
        entries.add(logEntry)
        flush(entries)
    }

    fun deleteSince(index: Int) {
        entries = entries.slice(0 until index).toMutableList()
        flush(entries)
    }

    private fun flush(list: MutableList<LogEntry<T>>) {
        ObjectOutputStream(FileOutputStream(filename)).use { it.writeObject(list) }
    }

    private fun read(): MutableList<LogEntry<T>> {
        try {
            ObjectInputStream(FileInputStream(filename)).use {
                @Suppress("UNCHECKED_CAST")
                return it.readObject() as MutableList<LogEntry<T>>
            }
        } catch (e: Exception) {
            return mutableListOf()
        }
    }
}

// ----- Raft Messages -----
// Startup Message
data class Init(val agents: List<ActorRef>)

// Timer Messages
enum class Timers {
    Election, ElectionTimeout, Heartbeat, StepDown
}
class ElectionTime
class ElectionTimeout
class Heartbeat
class StepDown

// Protocol Messages
abstract class Msg (val term: Int)
abstract class Request (term: Int) : Msg(term)
abstract class Response (term: Int, val success: Boolean) : Msg(term)

class AppendEntries<T> (
    term: Int,
    val seqNum: Int,
    val leaderId: Int,
    val prevLogIndex: Int,
    val prevLogTerm: Int,
    val leaderCommitIndex: Int,
    val entry: LogEntry<T>?
): Request(term)

class RequestVote(
    term: Int,
    val candidateId: Int,
    val lastLogIndex: Int,
    val lastLogTerm: Int
): Request(term)

class VoteResponse (
    term: Int,
    success: Boolean
): Response(term, success)

class AppendResponse (
    term: Int,
    success: Boolean,
    val requestId: Int = -1,
    val lastLogIndex: Int = 0
): Response(term, success)


// ----- Test Client -----

class Pong(val id: Int)
class Ping(val id: Int)
class Client: AbstractActorWithTimers() {
    private lateinit var agents: List<ActorRef>
    private var getCounter = 0
    private var setCounter = 0

    override fun createReceive(): Receive = receiveBuilder()
        .match(Init::class.java) { raft ->
            agents = raft.agents
            println("Client: Sending First command to first agent")
            agents[0].tell(RaftRequest(RaftRequestType.Set, Command("ssn", "1234")), self)
        }
        .match(RaftResponse::class.java) { res->
            val actualLeader = res.leader
            if (res.value != null) // must be reply to set
                println("Client: Response: ${res.value}")

            timers.startPeriodicTimer("setTest", Ping(actualLeader), Duration.ofMillis(500))
            timers.startPeriodicTimer("getTest", Pong(actualLeader), Duration.ofMillis(1000))
        }
        .match(Ping::class.java) {
            agents[it.id].tell(RaftRequest(RaftRequestType.Set, Command(setCounter.toString(), setCounter.toString()+setCounter.toString())), self)
            setCounter++
        }
        .match(Pong::class.java) {
            agents[it.id].tell(RaftRequest(RaftRequestType.Get, Command(getCounter.toString(), "")), self)
            getCounter++
        }
        .build()
}

fun main() {
    val numActors = 7
    println("Raft starting up!")
    val system = ActorSystem.create("Raft")
    val list = (0 until numActors).map{ system.actorOf(Props.create(RaftAgent::class.java) {RaftAgent<String>()}, it.toString())}
    list.map{ it.tell(Init(list), ActorRef.noSender()) }

    val client = system.actorOf(Props.create(Client::class.java))
    // Test Client is dumb, doesn't retry on No-Leader states
    // Giving it time for a leader to be elected
    Thread.sleep(5000)
    client.tell(Init(list), ActorRef.noSender())
}
