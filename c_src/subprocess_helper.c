/* Arbor RPC native subprocess helper. Protocol version 1; source-build draft.
 * Protocol: big-endian uint32 packet length followed by type+payload.
 * Input I=stdin bytes, A=one output credit, H=finite owner lease,
 * C=cleanup, Q=exit after cleanup; all carry the broker's uint64 token.
 * No command contains or accepts an OS PID or PGID.
 * Output S=start proof, D=stdout bytes, O=observed exit (still unreaped),
 * F=stdout EOF, R=cleanup receipt, E=protocol rejection.
 */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char **environ;
#define CHUNK 16384
#define VERSION 1
#define MAX_WRITE_CAP (64U * 1024U * 1024U)
#define SLOT_COUNT 10

struct packet { unsigned char bytes[CHUNK + 32]; size_t size, sent; bool data; };
struct broker {
  pid_t child;
  uint64_t token;
  bool group, owns, observed, announced, closing, killed, finished, input_eof;
  bool output_eof, disconnected, quit, confirmed, probe_complete, targeted_group_gone;
  int child_in, child_out, exec_status, status, signals;
  int64_t lease_at, term_at, cleanup_at, retire_at;
  unsigned grace_ms, budget_ms, lease_ms, retention_ms, write_cap, startup_ms;
  uint64_t data_sequence, acknowledged_sequence, write_sequence;
  bool credit_started, merge_stderr;
  unsigned credit;
  unsigned char *incoming, *writing;
  size_t incoming_size, writing_size, writing_sent;
  struct packet packets[SLOT_COUNT];
  unsigned first, count;
};

static int64_t now_ms(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) abort();
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}
static void put32(unsigned char *p, uint32_t n) {
  n = htonl(n); memcpy(p, &n, 4);
}
static uint32_t get32(const unsigned char *p) {
  uint32_t n; memcpy(&n, p, 4); return ntohl(n);
}
static void put64(unsigned char *p, uint64_t n) {
  put32(p, (uint32_t)(n >> 32)); put32(p + 4, (uint32_t)n);
}
static uint64_t get64(const unsigned char *p) {
  return ((uint64_t)get32(p) << 32) | get32(p + 4);
}
static void close_fd(int *fd) { if (*fd >= 0) close(*fd); *fd = -1; }
static int nonblocking(int fd) {
  return fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
}
static int cloexec_pipe(int fds[2]) {
  if (pipe(fds) != 0) return -1;
  if (fcntl(fds[0], F_SETFD, FD_CLOEXEC) != 0 ||
      fcntl(fds[1], F_SETFD, FD_CLOEXEC) != 0) return -1;
  return 0;
}
static bool queue_packet(struct broker *b, unsigned char type,
                         const unsigned char *body, size_t size) {
  if (size > CHUNK + 16 || b->count == SLOT_COUNT) return false;
  struct packet *p = &b->packets[(b->first + b->count) % SLOT_COUNT];
  put32(p->bytes, (uint32_t)(size + 2)); p->bytes[4] = VERSION; p->bytes[5] = type;
  if (size) memcpy(p->bytes + 6, body, size);
  p->size = size + 6; p->sent = 0; p->data = type == 'D'; b->count++;
  return true;
}
static void rejection(struct broker *b, unsigned char reason) {
  unsigned char body[9]; put64(body, b->token); body[8] = reason;
  if (!queue_packet(b, 'E', body, sizeof(body))) b->disconnected = true;
}
static void flush_output(struct broker *b) {
  while (b->count && !b->disconnected) {
    struct packet *p = &b->packets[b->first];
    ssize_t n = write(STDOUT_FILENO, p->bytes + p->sent, p->size - p->sent);
    if (n > 0) p->sent += (size_t)n;
    else if (n < 0 && errno == EINTR) continue;
    else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    else { b->disconnected = true; return; }
    if (p->sent == p->size) {
      b->first = (b->first + 1) % SLOT_COUNT; b->count--;
    }
  }
}
static bool observe(struct broker *b) {
  if (!b->owns || b->observed) return b->observed;
  siginfo_t info; memset(&info, 0, sizeof(info));
  if (waitid(P_PID, (id_t)b->child, &info, WEXITED | WNOHANG | WNOWAIT) != 0) {
    /* Lost ownership never permits a numeric signal fallback. */
    b->owns = false; b->status = -1; return false;
  }
  if (info.si_pid == b->child) {
    b->observed = true;
    b->status = info.si_code == CLD_EXITED ? info.si_status : 128 + info.si_status;
  }
  return b->observed;
}
static bool owned_signal(struct broker *b, int sig) {
  if (!b->owns || b->finished) return false;
  /* Parent alone reaps child; even a concurrently exited child remains
   * waitable, retaining this PID until all future signals are disabled. */
  int rc = kill(b->group ? -b->child : b->child, sig);
  if (rc == 0) b->signals++;
  return rc == 0 || errno == ESRCH;
}
static void receipt(struct broker *b, bool confirmed) {
  unsigned char body[22];
  put64(body, b->token); body[8] = confirmed ? 1 : 0;
  body[9] = b->group ? (b->targeted_group_gone ? 1 : 0) : (confirmed ? 1 : 0);
  put32(body + 10, (uint32_t)b->status);
  put32(body + 14, (uint32_t)b->signals);
  put32(body + 18, (uint32_t)b->child);
  if (!queue_packet(b, 'R', body, sizeof(body))) b->disconnected = true;
}
static void complete_receipt(struct broker *b) {
  b->probe_complete = true;
  receipt(b, b->confirmed);
  b->retire_at = now_ms() + b->retention_ms;
}
static void probe_reaped_group(struct broker *b) {
  if (!b->finished || b->owns || !b->confirmed || !b->group || b->probe_complete) return;
  /* Read-only AFTER all signal paths were disabled and the owned leader was
   * reaped. An occupied or reused numeric identity is conservative failure;
   * never signal it. ESRCH proves targeted group absence at this observation,
   * not containment of descendants that changed group/session. */
  if (now_ms() >= b->cleanup_at) { complete_receipt(b); return; }
  int result = kill(-b->child, 0);
  if (now_ms() >= b->cleanup_at) { complete_receipt(b); return; }
  if (result < 0 && errno == ESRCH) {
    b->targeted_group_gone = true;
    complete_receipt(b);
  }
}
static void finish(struct broker *b, bool confirmed) {
  /* Disable ALL signal paths before releasing PID reservation. */
  b->finished = true;
  if (confirmed && b->owns && b->observed) {
    int status;
    pid_t result;
    do { result = waitpid(b->child, &status, 0); } while (result < 0 && errno == EINTR);
    confirmed = result == b->child;
  }
  b->owns = false;
  b->confirmed = confirmed;
  if (confirmed && b->group) probe_reaped_group(b);
  else complete_receipt(b);
}
static void failed_signal(struct broker *b) {
  /* macOS may return EPERM for a group whose sole remaining leader is a
   * zombie. Actual owned-child exit still permits reaping followed only by
   * the read-only absence check; occupied/unknown groups stay unconfirmed. */
  bool exited = observe(b);
  finish(b, exited && b->owns);
}
static void begin_cleanup(struct broker *b) {
  if (b->finished) { if (b->probe_complete) receipt(b, b->confirmed); return; }
  if (b->closing) return;
  b->closing = true; close_fd(&b->child_in); b->writing_size = 0;
  int64_t now = now_ms();
  b->term_at = now + b->grace_ms; b->cleanup_at = now + b->budget_ms;
  observe(b);
  if (!b->owns) { finish(b, false); return; }
  if (!b->group && b->observed) { finish(b, true); return; }
  if (!owned_signal(b, SIGTERM)) { failed_signal(b); return; }
}
static void advance_cleanup(struct broker *b) {
  observe(b);
  int64_t now = now_ms();
  if (b->observed && !b->announced) {
    unsigned char body[12]; put64(body, b->token);
    put32(body + 8, (uint32_t)b->status);
    if (queue_packet(b, 'O', body, sizeof(body))) b->announced = true;
  }
  if (b->finished) { probe_reaped_group(b); return; }
  if (!b->closing && (b->disconnected || now >= b->lease_at)) begin_cleanup(b);
  if (!b->closing || b->finished) return;
  if (b->observed && (!b->group || b->killed)) { finish(b, true); return; }
  if (!b->killed && now >= b->term_at) {
    b->killed = true;
    if (!owned_signal(b, SIGKILL)) { failed_signal(b); return; }
  }
  observe(b);
  if (b->observed && (!b->group || b->killed)) finish(b, true);
  else if (now >= b->cleanup_at) finish(b, false);
}
static void write_reply(struct broker *b, uint64_t sequence, unsigned char result) {
  unsigned char body[17]; put64(body, b->token); put64(body + 8, sequence); body[16] = result;
  if (!queue_packet(b, 'W', body, sizeof(body))) { b->disconnected = true; begin_cleanup(b); }
}
static void handle_command(struct broker *b, const unsigned char *body, size_t size) {
  if (size < 10 || body[0] != VERSION || get64(body + 2) != b->token) {
    rejection(b, 1); begin_cleanup(b); return;
  }
  unsigned char type = body[1];
  if (type == 'C' && size == 10) { begin_cleanup(b); return; }
  if (type == 'Q' && size == 10) { b->quit = true; begin_cleanup(b); return; }
  if (type == 'H' && size == 10) {
    if (!b->closing) b->lease_at = now_ms() + b->lease_ms;
    return;
  }
  if (type == 'A' && size == 18 && b->credit == 0) {
    uint64_t sequence = get64(body + 10);
    if ((!b->credit_started && sequence == 0) ||
        (sequence == b->data_sequence && sequence > b->acknowledged_sequence)) {
      b->credit_started = true; b->acknowledged_sequence = sequence; b->credit = 1; return;
    }
    rejection(b, 4); begin_cleanup(b); return;
  }
  if (type == 'I' && size >= 18) {
    uint64_t sequence = get64(body + 10);
    if (sequence == 0 || sequence <= b->write_sequence) { rejection(b, 4); begin_cleanup(b); return; }
    b->write_sequence = sequence;
    if (b->closing || b->input_eof || b->child_in < 0) { write_reply(b, sequence, 2); return; }
    size_t bytes = size - 18;
    if (b->writing_sent > 0) {
      memmove(b->writing, b->writing + b->writing_sent, b->writing_size - b->writing_sent);
      b->writing_size -= b->writing_sent; b->writing_sent = 0;
    }
    if (bytes > b->write_cap - b->writing_size) { write_reply(b, sequence, 1); return; }
    memcpy(b->writing + b->writing_size, body + 18, bytes); b->writing_size += bytes;
    write_reply(b, sequence, 0); return;
  }
  rejection(b, 3); begin_cleanup(b);
}
static void read_commands(struct broker *b) {
  size_t left = (size_t)b->write_cap + 22 - b->incoming_size;
  if (!left) { rejection(b, 2); begin_cleanup(b); return; }
  ssize_t n = read(STDIN_FILENO, b->incoming + b->incoming_size, left);
  if (n == 0) { b->disconnected = true; begin_cleanup(b); return; }
  if (n < 0) {
    if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
      b->disconnected = true; begin_cleanup(b);
    }
    return;
  }
  b->incoming_size += (size_t)n;
  while (b->incoming_size >= 4) {
    uint32_t size = get32(b->incoming);
    if (size < 10 || size > b->write_cap + 18) {
      rejection(b, 2); begin_cleanup(b); b->incoming_size = 0; return;
    }
    if (b->incoming_size < (size_t)size + 4) return;
    handle_command(b, b->incoming + 4, size);
    size_t consumed = (size_t)size + 4;
    memmove(b->incoming, b->incoming + consumed, b->incoming_size - consumed);
    b->incoming_size -= consumed;
  }
}
static void vendor_output(struct broker *b) {
  unsigned char data[CHUNK + 16];
  ssize_t n = read(b->child_out, data + 16, CHUNK);
  if (n > 0) {
    put64(data, b->token); put64(data + 8, ++b->data_sequence);
    if (!queue_packet(b, 'D', data, (size_t)n + 16)) { begin_cleanup(b); return; }
    b->credit = 0;
  } else if (n == 0) {
    close_fd(&b->child_out); b->output_eof = true;
    unsigned char token[8]; put64(token, b->token);
    if (!queue_packet(b, 'F', token, sizeof(token))) b->disconnected = true;
  } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
    close_fd(&b->child_out); begin_cleanup(b);
  }
}
static void vendor_input(struct broker *b) {
  if (b->writing_sent < b->writing_size) {
    ssize_t n = write(b->child_in, b->writing + b->writing_sent,
                      b->writing_size - b->writing_sent);
    if (n > 0) b->writing_sent += (size_t)n;
    else if (n < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
      close_fd(&b->child_in); b->writing_size = b->writing_sent = 0;
    }
  }
  if (b->input_eof && b->writing_sent == b->writing_size) close_fd(&b->child_in);
}
static unsigned parse_ms(const char *s) {
  char *end; unsigned long n = strtoul(s, &end, 10);
  if (!*s || *end || n > 60000) { fprintf(stderr, "invalid duration\n"); exit(2); }
  return (unsigned)n;
}
int main(int argc, char **argv) {
  if (argc < 12 || strcmp(argv[1], "1") != 0 || strcmp(argv[10], "--") != 0) {
    fprintf(stderr, "invalid Arbor RPC helper protocol or startup arguments\n"); return 2;
  }
  struct broker b; memset(&b, 0, sizeof(b));
  b.child_in = b.child_out = -1; b.status = -1;
  if (strcmp(argv[2], "0") != 0 && strcmp(argv[2], "1") != 0) return 2;
  if (strcmp(argv[3], "0") != 0 && strcmp(argv[3], "1") != 0) return 2;
  b.group = strcmp(argv[2], "1") == 0; b.merge_stderr = strcmp(argv[3], "1") == 0;
  b.grace_ms = parse_ms(argv[4]); b.budget_ms = parse_ms(argv[5]);
  b.lease_ms = parse_ms(argv[6]); b.retention_ms = parse_ms(argv[7]);
  b.startup_ms = parse_ms(argv[9]);
  char *end; unsigned long cap = strtoul(argv[8], &end, 10);
  if (!*argv[8] || *end || cap == 0 || cap > MAX_WRITE_CAP) return 2;
  b.write_cap = (unsigned)cap;
  if (b.budget_ms < b.grace_ms + 50 || !b.lease_ms || !b.retention_ms || !b.startup_ms) return 2;
  b.incoming = malloc((size_t)b.write_cap + 22); b.writing = malloc(b.write_cap);
  if (!b.incoming || !b.writing) return 2;
  b.lease_at = now_ms() + b.lease_ms;
  int random_fd = open("/dev/urandom", O_RDONLY);
  if (random_fd < 0 || read(random_fd, &b.token, sizeof(b.token)) != sizeof(b.token)) return 2;
  close(random_fd);
  struct sigaction sa; memset(&sa, 0, sizeof(sa)); sigemptyset(&sa.sa_mask);
  sa.sa_handler = SIG_DFL; if (sigaction(SIGCHLD, &sa, NULL) != 0) return 2;
  sa.sa_handler = SIG_IGN; if (sigaction(SIGPIPE, &sa, NULL) != 0) return 2;
  int in[2], out[2], exec_ack[2];
  if (cloexec_pipe(in) || cloexec_pipe(out) || cloexec_pipe(exec_ack)) return 2;
  b.child = fork();
  if (b.child < 0) return 2;
  if (b.child == 0) {
    close(in[1]); close(out[0]); close(exec_ack[0]);
    sigset_t empty; sigemptyset(&empty); sigprocmask(SIG_SETMASK, &empty, NULL);
    sa.sa_handler = SIG_DFL; sigaction(SIGPIPE, &sa, NULL);
    if (setsid() < 0 || dup2(in[0], 0) < 0 || dup2(out[1], 1) < 0 ||
        (b.merge_stderr && dup2(out[1], 2) < 0)) {
      int error = errno; (void)write(exec_ack[1], &error, sizeof(error)); _exit(127);
    }
    close(in[0]); close(out[1]);
    /* This trusted setup marker precedes exec, keeping fast exits identifiable. */
    int ready = 0; (void)write(exec_ack[1], &ready, sizeof(ready));
    execve(argv[11], &argv[11], environ);
    int error = errno; (void)write(exec_ack[1], &error, sizeof(error)); _exit(127);
  }
  b.owns = true; close(in[0]); close(out[1]); close(exec_ack[1]);
  b.child_in = in[1]; b.child_out = out[0];
  /* The trusted child is not reaped by a handler or another thread. */
  struct pollfd ack = {.fd = exec_ack[0], .events = POLLIN | POLLHUP};
  int ready = -1; int64_t startup_at = now_ms() + b.startup_ms;
  bool setup_ok = false;
  int remaining = (int)(startup_at - now_ms());
  if (remaining > 0 && poll(&ack, 1, remaining) > 0 &&
      read(exec_ack[0], &ready, sizeof(ready)) == sizeof(ready) && ready == 0 &&
      (!b.group || (getpgid(b.child) == b.child && getsid(b.child) == b.child))) {
    remaining = (int)(startup_at - now_ms());
    int error = 0;
    if (remaining > 0 && poll(&ack, 1, remaining) > 0) {
      ssize_t n = read(exec_ack[0], &error, sizeof(error));
      if (n == 0) setup_ok = true;
      else b.exec_status = n == sizeof(error) ? error : EIO;
    } else b.exec_status = ETIMEDOUT;
  } else b.exec_status = ready > 0 ? ready : EIO;
  close(exec_ack[0]);
  nonblocking(0); nonblocking(1); nonblocking(b.child_in); nonblocking(b.child_out);
  if (!setup_ok) b.group = false;
  unsigned char start[21]; put64(start, b.token); put32(start + 8, (uint32_t)b.child);
  put32(start + 12, (uint32_t)(b.group ? b.child : 0)); start[16] = b.group ? 1 : 0;
  put32(start + 17, (uint32_t)b.exec_status); queue_packet(&b, 'S', start, sizeof(start));
  if (!setup_ok) begin_cleanup(&b);
  while (true) {
    advance_cleanup(&b);
    flush_output(&b);
    if (b.finished && b.probe_complete &&
        (b.disconnected || (b.quit && !b.count) || now_ms() >= b.retire_at)) break;
    struct pollfd fds[4] = {
      {.fd = b.disconnected ? -1 : 0, .events = POLLIN | POLLHUP},
      {.fd = b.disconnected || !b.count ? -1 : 1, .events = POLLOUT},
      {.fd = b.credit && b.count < SLOT_COUNT - 4 ? b.child_out : -1, .events = POLLIN | POLLHUP},
      {.fd = b.writing_sent < b.writing_size || b.input_eof ? b.child_in : -1, .events = POLLOUT}
    };
    int result = poll(fds, 4, 5);
    if (result < 0 && errno != EINTR) { b.disconnected = true; begin_cleanup(&b); }
    if (fds[0].revents) read_commands(&b);
    if (fds[1].revents) flush_output(&b);
    if (fds[2].revents) vendor_output(&b);
    if (fds[3].revents) vendor_input(&b);
  }
  close_fd(&b.child_in); close_fd(&b.child_out);
  free(b.incoming); free(b.writing);
  return b.observed ? 0 : 3;
}
