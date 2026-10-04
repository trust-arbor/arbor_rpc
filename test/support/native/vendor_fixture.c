/* Native fixture avoids shell/Python mutations of the supplied environment. */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
extern char **environ;
static int write_bytes(const void *data, size_t size) {
  const unsigned char *p = data;
  while (size) {
    ssize_t n = write(1, p, size);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return -1;
    p += n; size -= (size_t)n;
  }
  return 0;
}
static void write32(uint32_t n) {
  unsigned char bytes[4] = {(unsigned char)(n >> 24), (unsigned char)(n >> 16),
                            (unsigned char)(n >> 8), (unsigned char)n};
  write_bytes(bytes, sizeof(bytes));
}
static void write_string(const char *s) {
  size_t n = strlen(s); write32((uint32_t)n); write_bytes(s, n);
}
int main(int argc, char **argv) {
  if (argc < 2) return 2;
  if (strcmp(argv[1], "exact") == 0) {
    write32((uint32_t)argc);
    for (int i = 0; i < argc; i++) write_string(argv[i]);
    uint32_t count = 0; for (char **e = environ; *e; e++) count++;
    write32(count); for (char **e = environ; *e; e++) write_string(*e);
    char cwd[4096]; if (!getcwd(cwd, sizeof(cwd))) return 2;
    write_string(cwd);
    unsigned char bytes[4096]; ssize_t n;
    const char *expected = getenv("FIXTURE_BYTES");
    size_t left = expected ? (size_t)strtoul(expected, NULL, 10) : (size_t)-1;
    while (left && (n = read(0, bytes, left < sizeof(bytes) ? left : sizeof(bytes))) > 0) {
      if (write_bytes(bytes, (size_t)n)) return 2;
      if (expected) left -= (size_t)n;
    }
    return 23;
  }
  if (strcmp(argv[1], "fast") == 0) { write_bytes("final\xff\r\n", 8); return 7; }
  if (strcmp(argv[1], "ignore-term") == 0) {
    signal(SIGTERM, SIG_IGN); write_bytes("ready\n", 6);
    while (true) pause();
  }
  if (strcmp(argv[1], "group") == 0) {
    signal(SIGTERM, SIG_IGN);
    pid_t descendant = fork();
    if (descendant < 0) return 2;
    if (descendant == 0) { while (true) pause(); }
    char pid[32]; int n = snprintf(pid, sizeof(pid), "%ld\n", (long)descendant);
    write_bytes(pid, (size_t)n);
    if (argc > 2 && strcmp(argv[2], "exit-leader") == 0) return 0;
    while (true) pause();
  }
  if (strcmp(argv[1], "escaped") == 0) {
    signal(SIGTERM, SIG_IGN);
    pid_t descendant = fork();
    if (descendant < 0) return 2;
    if (descendant == 0) {
      if (setsid() < 0) return 2;
      char pid[32]; int n = snprintf(pid, sizeof(pid), "%ld\n", (long)getpid());
      write_bytes(pid, (size_t)n);
      /* Finite self-expiry avoids relying on a numeric fixture-cleanup signal. */
      struct timespec delay = {.tv_sec = 1, .tv_nsec = 0};
      while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
      return 0;
    }
    return 0;
  }
  if (strcmp(argv[1], "flood") == 0) {
    unsigned char bytes[16384]; memset(bytes, 'x', sizeof(bytes));
    while (write_bytes(bytes, sizeof(bytes)) == 0) {}
    return 0;
  }
  return 2;
}
