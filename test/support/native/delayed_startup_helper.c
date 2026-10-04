/* Test-only launch barrier; the production helper has no delay/test option. */
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
static int delayed_execve(const char *path, char *const argv[], char *const env[]) {
  const char *marker = getenv("ARBOR_TEST_EXEC_MARKER");
  if (marker) {
    FILE *file = fopen(marker, "w");
    if (file) { fprintf(file, "%ld", (long)getpid()); fclose(file); }
  }
  struct timespec delay = {.tv_sec = 0, .tv_nsec = 300000000};
  while (nanosleep(&delay, &delay) != 0) {}
  return execve(path, argv, env);
}
#define execve delayed_execve
#include "../../../c_src/subprocess_helper.c"
