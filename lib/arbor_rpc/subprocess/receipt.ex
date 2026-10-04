defmodule Arbor.RPC.Subprocess.Receipt do
  @moduledoc """
  A retained cleanup observation for one owned child generation.

  `direct_child: :reaped` records actual owned-child exit and reaping. When a
  private process group was requested, `targeted_group: :absent` records its
  absence after signalling was permanently disabled and its leader was reaped.
  This does not establish containment of descendants that changed group/session.
  Lost helper or guardian identity, an expired deadline, and a missing receipt
  cannot establish cleanup success.
  """

  @enforce_keys [:generation, :direct_child, :targeted_group, :completed_at]
  defstruct [
    :generation,
    :direct_child,
    :targeted_group,
    :completed_at,
    :vendor_status,
    :helper_status,
    :reason,
    backend: :native,
    descendant_tree: :not_contained
  ]

  @type t :: %__MODULE__{
          generation: reference(),
          direct_child: :reaped | :unconfirmed,
          targeted_group: :not_requested | :absent | :unconfirmed,
          descendant_tree: :not_contained,
          vendor_status: non_neg_integer() | nil,
          helper_status: non_neg_integer() | nil,
          reason: term(),
          backend: :native,
          completed_at: integer()
        }

  @spec result(t()) :: :ok | {:error, term()}
  def result(%__MODULE__{reason: reason}) when not is_nil(reason), do: {:error, reason}

  def result(%__MODULE__{direct_child: :reaped, targeted_group: group})
      when group in [:not_requested, :absent],
      do: :ok

  def result(%__MODULE__{direct_child: :unconfirmed}), do: {:error, :cleanup_unconfirmed}
  def result(%__MODULE__{}), do: {:error, :targeted_group_cleanup_unconfirmed}

  @doc false
  def unconfirmed(generation, group?, reason, helper_status \\ nil) do
    %__MODULE__{
      generation: generation,
      direct_child: :unconfirmed,
      targeted_group: if(group?, do: :unconfirmed, else: :not_requested),
      completed_at: System.monotonic_time(:millisecond),
      reason: reason,
      helper_status: helper_status
    }
  end
end
