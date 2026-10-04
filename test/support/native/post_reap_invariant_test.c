/* Test-only syscall seam: an occupied/reused PGID must NEVER get a signal. */
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/types.h>

static int mode, probes, actual_signals;
static int simulated_kill(pid_t pid, int sig) {
  assert(pid == -12345);
  if (sig != 0) { actual_signals++; return 0; }
  probes++;
  if (mode == 0) return 0; /* occupied or reused numeric group */
  errno = mode == 1 ? ESRCH : EPERM;
  return -1;
}
#define kill simulated_kill
#define main unused_broker_entrypoint
#include "../../../c_src/subprocess_helper.c"
#undef main
#undef kill

static void scenario(int probe_mode, bool gone) {
  mode = probe_mode; probes = actual_signals = 0;
  struct broker b; memset(&b, 0, sizeof(b));
  b.child = 12345; b.group = true; b.finished = true; b.confirmed = true;
  b.observed = true; b.status = 0; b.signals = 2; b.cleanup_at = now_ms() + 100;
  assert(!owned_signal(&b, SIGTERM));
  assert(!owned_signal(&b, SIGKILL));
  probe_reaped_group(&b);
  if (!gone) {
    assert(!b.probe_complete);
    b.cleanup_at = now_ms() - 1;
    probe_reaped_group(&b);
  }
  assert(b.probe_complete && b.targeted_group_gone == gone);
  assert(probes == 1 && actual_signals == 0 && b.signals == 2);
  assert(b.count == 1 && b.packets[b.first].bytes[5] == 'R');
  assert(b.packets[b.first].bytes[14] == 1); /* direct child confirmed */
  assert(b.packets[b.first].bytes[15] == (gone ? 1 : 0));
  begin_cleanup(&b); /* Repeat close reads receipt; no signal/probe is repeated. */
  assert(probes == 1 && actual_signals == 0 && b.signals == 2);
  assert(!owned_signal(&b, SIGKILL));
}
int main(void) {
  scenario(0, false);
  scenario(1, true);
  scenario(2, false);
  struct broker expired; memset(&expired, 0, sizeof(expired));
  expired.child = 12345; expired.group = expired.finished = expired.confirmed = true;
  expired.cleanup_at = now_ms() - 1; probes = actual_signals = 0; mode = 1;
  probe_reaped_group(&expired);
  assert(expired.probe_complete && !expired.targeted_group_gone);
  assert(probes == 0 && actual_signals == 0);
  puts("post-reap invariant: occupied, absent, permission-denied and expired cases pass; zero signals");
  return 0;
}
