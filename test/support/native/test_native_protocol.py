"""Real-process checks against the shipped native source, built before this suite."""
import os
import select
import struct
import subprocess
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BROKER = str(ROOT.parents[2] / "priv/native/arbor_rpc_subprocess")
VENDOR = str(ROOT / "vendor_fixture")


def decode_fixture(output):
    offset = 0

    def number():
        nonlocal offset
        n = struct.unpack(">I", output[offset:offset + 4])[0]
        offset += 4
        return n

    def string():
        nonlocal offset
        n = number()
        value = output[offset:offset + n]
        offset += n
        return value

    argv = [string() for _ in range(number())]
    env = [string() for _ in range(number())]
    cwd = string()
    return argv, env, cwd, output[offset:]


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def eventually_gone(pid, timeout=2):
    cutoff = time.monotonic() + timeout
    while time.monotonic() < cutoff:
        if not alive(pid):
            return True
        time.sleep(0.01)
    return False


class Controller:
    def __init__(self, mode, *args, group=False, env=None, cwd=None, lease=5000):
        self.proc = subprocess.Popen(
            [BROKER, "1", str(int(group)), "0", "50", "500", str(lease), "5000", "1048576", "1000", "--", VENDOR, mode, *args],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, cwd=cwd, bufsize=0,
        )
        self.sequence = 0
        self.write_sequence = 0
        self.buffer = bytearray()
        self.pending = []
        kind, body = self.receive()
        assert kind == "S", (kind, body)
        self.token, self.pid, self.pgid, self.group, self.exec_error = struct.unpack(">QIIBI", body)
        assert self.exec_error == 0, self.exec_error

    def command(self, kind, body=b"", token=None):
        if kind == "A":
            body = struct.pack(">Q", self.sequence) + body
        if kind == "I":
            self.write_sequence += 1
            body = struct.pack(">Q", self.write_sequence) + body
        packet = b"\x01" + kind.encode() + struct.pack(">Q", self.token if token is None else token) + body
        self.proc.stdin.write(struct.pack(">I", len(packet)) + packet)
        self.proc.stdin.flush()

    def receive(self, timeout=2):
        cutoff = time.monotonic() + timeout
        while True:
            if len(self.buffer) >= 4:
                size = struct.unpack(">I", self.buffer[:4])[0]
                if len(self.buffer) >= size + 4:
                    packet = bytes(self.buffer[4: size + 4])
                    del self.buffer[:size + 4]
                    assert packet[0] == 1, packet
                    kind, body = chr(packet[1]), packet[2:]
                    if kind == "D":
                        token, sequence = struct.unpack(">QQ", body[:16])
                        assert token == self.token and sequence == self.sequence + 1
                        self.sequence = sequence
                        body = body[16:]
                    elif kind in ("O", "F", "E"):
                        assert struct.unpack(">Q", body[:8])[0] == self.token
                        body = body[8:]
                    return kind, body
            left = cutoff - time.monotonic()
            if left <= 0:
                raise TimeoutError("broker packet deadline")
            if select.select([self.proc.stdout], [], [], left)[0]:
                data = os.read(self.proc.stdout.fileno(), 32768)
                if not data:
                    raise EOFError(self.proc.stderr.read())
                self.buffer.extend(data)

    def until(self, kind, timeout=2):
        cutoff = time.monotonic() + timeout
        while True:
            event = self.receive(max(0, cutoff - time.monotonic()))
            if event[0] == kind:
                return event[1]
            self.pending.append(event)

    def collect(self):
        self.command("A")
        output = bytearray()
        while True:
            kind, body = self.receive()
            if kind == "D":
                output.extend(body)
                self.command("A")
            elif kind == "F":
                return bytes(output)
            else:
                self.pending.append((kind, body))

    def cleanup(self):
        self.command("C")
        return struct.unpack(">QBBIII", self.until("R"))

    def dispose(self):
        if self.proc.poll() is None:
            try:
                self.command("Q")
                self.proc.wait(timeout=2)
            except (BrokenPipeError, TimeoutError, subprocess.TimeoutExpired):
                self.proc.kill()
                self.proc.wait(timeout=2)
        self.proc.stdin.close()
        self.proc.stdout.close()
        self.proc.stderr.close()


class BrokerTests(unittest.TestCase):
    def test_exact_argv_env_cwd_binary_input_output_and_status(self):
        env = {"PATH": "/no-search-needed", "PWD": "literal", "SHLVL": "42", "_": "literal-underscore",
               "SECRET": "space\nquotes'\"$(not-evaluated)", "EMPTY": ""}
        args = ["", "space argument", "quote'\"", "$HOME", "line\nbreak", "arbitrary-\u03bb"]
        data = bytes(range(256)) * 32 + b"\x00\xff\r\nfinal"
        env["FIXTURE_BYTES"] = str(len(data))
        c = Controller("exact", *args, env=env, cwd=ROOT)
        try:
            c.command("I", data)
            output = c.collect()
            argv_part, env_part, cwd_part, stdin_part = decode_fixture(output)
            self.assertEqual(argv_part, [os.fsencode(VENDOR), b"exact", *map(os.fsencode, args)])
            self.assertEqual(set(env_part), {os.fsencode(k + "=" + v) for k, v in env.items()})
            self.assertEqual(cwd_part, os.fsencode(ROOT))
            self.assertEqual(stdin_part, data)
            _, direct, requested_scope, status, signals, pid = c.cleanup()
            self.assertEqual((direct, requested_scope, status, signals, pid), (1, 1, 23, 0, c.pid))
            self.assertFalse(alive(c.pid))
        finally:
            c.dispose()

    def test_fast_exit_remains_unreaped_during_delayed_guardian(self):
        c = Controller("fast")
        decoy = subprocess.Popen(["/bin/sleep", "10"])
        try:
            self.assertEqual(c.collect(), b"final\xff\r\n")
            # A guardian can pause arbitrarily within the lease: the native
            # parent retains this exited child rather than releasing its PID.
            time.sleep(0.15)
            self.assertTrue(alive(c.pid))
            state = subprocess.check_output(["/bin/ps", "-o", "stat=", "-p", str(c.pid)]).decode().strip()
            self.assertIn("Z", state)
            before = c.cleanup()
            self.assertEqual(before[1:5], (1, 1, 7, 0))
            self.assertFalse(alive(c.pid))
            # A late repeated command is a receipt lookup, never a new signal.
            after = c.cleanup()
            self.assertEqual(after, before)
            self.assertIsNone(decoy.poll())
        finally:
            c.dispose(); decoy.terminate(); decoy.wait(timeout=2)

    def test_stale_token_closes_owned_child_but_cannot_target_decoy(self):
        c = Controller("ignore-term")
        decoy = subprocess.Popen(["/bin/sleep", "10"])
        try:
            c.command("A"); self.assertEqual(c.until("D"), b"ready\n")
            c.command("C", token=c.token ^ 1)
            self.assertEqual(c.until("E"), b"\x01")
            self.assertIsNone(decoy.poll())
            c.command("C", struct.pack(">I", decoy.pid))
            self.assertEqual(c.until("E"), b"\x03")
            self.assertIsNone(decoy.poll())
            started = time.monotonic()
            result = c.cleanup()
            self.assertEqual(result[1:5], (1, 1, 137, 2))
            self.assertLess(time.monotonic() - started, 0.6)
            self.assertFalse(alive(c.pid)); self.assertIsNone(decoy.poll())
        finally:
            c.dispose(); decoy.terminate(); decoy.wait(timeout=2)

    def test_owned_group_confirms_targeted_absence_after_real_descendant_exit(self):
        c = Controller("group", group=True)
        try:
            self.assertEqual((c.group, c.pgid), (1, c.pid))
            c.command("A"); descendant = int(c.until("D"))
            self.assertEqual(os.getpgid(descendant), c.pgid)
            result = c.cleanup()
            self.assertEqual(result[1:5], (1, 1, 137, 2))
            self.assertFalse(alive(c.pid))
            self.assertTrue(eventually_gone(descendant))
        finally:
            c.dispose()

    def test_group_leader_exit_keeps_group_identity_until_last_signal(self):
        c = Controller("group", "exit-leader", group=True)
        decoy = subprocess.Popen(["/bin/sleep", "10"])
        try:
            c.command("A"); descendant = int(c.until("D"))
            c.until("O")
            time.sleep(0.15)
            self.assertTrue(alive(c.pid)); self.assertTrue(alive(descendant))
            self.assertEqual(os.getpgid(descendant), c.pid)
            result = c.cleanup()
            self.assertEqual(result[1:5], (1, 1, 0, 2))
            self.assertFalse(alive(c.pid)); self.assertTrue(eventually_gone(descendant))
            self.assertIsNone(decoy.poll())
            c.command("C", struct.pack(">I", decoy.pid))
            self.assertEqual(c.until("E"), b"\x03")
            self.assertEqual(c.cleanup(), result)
            self.assertIsNone(decoy.poll())
        finally:
            c.dispose(); decoy.terminate(); decoy.wait(timeout=2)

    def test_owner_control_eof_runs_finite_cleanup_without_resume(self):
        c = Controller("ignore-term")
        try:
            c.command("A"); self.assertEqual(c.until("D"), b"ready\n")
            c.proc.stdin.close()
            c.proc.wait(timeout=2)
            self.assertFalse(alive(c.pid))
            self.assertEqual(c.proc.returncode, 0)
        finally:
            c.dispose()

    def test_targeted_group_absence_does_not_claim_escaped_descendant_cleanup(self):
        c = Controller("escaped", group=True)
        try:
            c.command("A"); descendant = int(c.until("D"))
            self.assertEqual(os.getpgid(descendant), descendant)
            result = c.cleanup()
            self.assertEqual(result[1:4], (1, 1, 0))
            self.assertLessEqual(result[4], 2)
            self.assertFalse(alive(c.pid))
            self.assertTrue(alive(descendant))
            self.assertTrue(eventually_gone(descendant))
        finally:
            c.dispose()

    def test_no_credit_does_not_block_cleanup_control_lane(self):
        c = Controller("flood")
        try:
            time.sleep(0.1)
            # No A credit: native vendor pipe can fill, but no raw data packet
            # is emitted to the guardian and cleanup still progresses.
            result = c.cleanup()
            self.assertEqual(result[1], 1)
            self.assertFalse(alive(c.pid))
            self.assertFalse(any(kind == "D" for kind, _ in c.pending))
        finally:
            c.dispose()

    def test_oversized_declared_command_is_rejected_before_body_allocation(self):
        c = Controller("ignore-term")
        decoy = subprocess.Popen(["/bin/sleep", "10"])
        try:
            c.command("A"); self.assertEqual(c.until("D"), b"ready\n")
            c.proc.stdin.write(struct.pack(">I", 1048576 + 19))
            c.proc.stdin.flush()
            self.assertEqual(c.until("E"), b"\x02")
            result = struct.unpack(">QBBIII", c.until("R"))
            self.assertEqual(result[1:4], (1, 1, 137))
            self.assertFalse(alive(c.pid)); self.assertIsNone(decoy.poll())
        finally:
            c.dispose(); decoy.terminate(); decoy.wait(timeout=2)

    def test_duplicate_native_credit_is_rejected_and_owned_child_is_reaped(self):
        c = Controller("flood")
        try:
            c.command("A"); self.assertEqual(len(c.until("D")), 16384)
            # Direct malformed protocol reuse, not the sequence-aware facade.
            packet = b"\x01A" + struct.pack(">QQ", c.token, 0)
            c.proc.stdin.write(struct.pack(">I", len(packet)) + packet)
            c.proc.stdin.flush()
            self.assertEqual(c.until("E"), b"\x04")
            result = struct.unpack(">QBBIII", c.until("R"))
            self.assertEqual(result[1], 1)
            self.assertFalse(alive(c.pid))
        finally:
            c.dispose()



if __name__ == "__main__":
    unittest.main(verbosity=2)
