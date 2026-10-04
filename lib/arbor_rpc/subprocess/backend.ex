defmodule Arbor.RPC.Subprocess.Backend do
  @moduledoc false

  alias Arbor.RPC.Subprocess.Command

  @callback open(Command.t(), keyword(), map()) :: {:ok, port()} | {:error, term()}
  @callback command(port(), binary()) :: :ok | {:error, term()}
  @callback close(port()) :: :ok
end
