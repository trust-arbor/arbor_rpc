defmodule Arbor.RPC.Subprocess.Cleanup do
  @moduledoc false

  # Numeric PID/PGID proofs cannot retain process identity through reap. The
  # native backend is the only supported cleanup authority. These retired
  # private entry points never signal or close a caller-supplied Port.
  def proof(_port, _group, _timeout), do: {:error, :unsupported_unmanaged_cleanup}
  def run(_proof, _port, _opts), do: {:error, :unsupported_unmanaged_cleanup}
  def after_exit(_proof, _port, _opts), do: {:error, :unsupported_unmanaged_cleanup}

  @doc false
  def classify_probe({:ok, _output, 0}), do: :alive

  def classify_probe({:ok, output, status}) when is_binary(output) do
    # Both fixed Unix kill utilities report ESRCH in this form under LC_ALL=C.
    # EPERM and utility/argument errors are unknown, never proof of exit.
    if String.contains?(output, "No such process"),
      do: :gone,
      else: {:error, {:liveness_unconfirmed, status}}
  end

  def classify_probe(error), do: error
end
