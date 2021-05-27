defmodule PriorityQueue do

if Application.get_env(:priority_queue, :native) do
  @compile :native
  @compile {:hipe, [:o3]}
end


  @moduledoc """
  Priority Queues in Elixir

  From Wikipedia: A priority queue is an abstract data type which is like
  a regular queue or stack data structure, but where additionally each
  element has a "priority" associated with it. In a priority queue, an
  element with high priority is served before an element with low
  priority. If two elements have the same priority, they are served
  according to their order in the queue.

  While priority queues are often implemented with heaps, they are
  conceptually distinct from heaps. A priority queue is an abstract
  concept like "a list" or "a map"; just as a list can be implemented with
  a linked list or an array, a priority queue can be implemented with a
  heap or a variety of other methods.

  In computer science, a heap is a specialized tree-based data structure
  that satisfies the heap property:
  *  If A is a parent node of B, then the key of node A is ordered with
     respect to the key of node B, with the same ordering applying
     across the heap.

  a) Either the keys of parent nodes are always greater than or equal to
     those of the children and the highest key is in the root node (this
     kind of heap is called max heap)
  b) or the keys of parent nodes are less than or equal to those of the
     children and the lowest key is in the root node (min heap)

  The heap is one maximally efficient implementation of a Priority Queue

  %% heap :: nil | {Item, Value, [heap()]}
  """

  @type key :: any
  @type value :: any
  @type element :: {key, value}

  @heap PairingHeap


  @type t :: %__MODULE__{heap: term, key_set: map}
  defstruct heap: nil, key_set: MapSet.new

  #
  # Public interface: priority queues
  #

  @doc """
  Return new priority queue with minimum element removed

      iex> [{4, []}, {8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.delete_min |> PriorityQueue.size
      3

      iex> [{1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.delete_min |> PriorityQueue.delete_min |> PriorityQueue.size
      0
  """
  @spec delete_min(t) :: t
  def delete_min(pq = %__MODULE__{heap: {k, _, _} = heap, key_set: set}) do
    case empty?(pq) do
      true -> pq
      _    -> %{pq | heap: @heap.delete_min(heap), key_set: MapSet.delete(set, k)}
    end
  end
  def delete_min(pq = %__MODULE__{heap: nil, key_set: _set}), do: pq

  @doc """
  Return new priority queue with minimum element removed

  Raises a PriorityQueue.EmptyError exception if the queue is empty

      iex> [{4, []} ,{8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.delete_min |> PriorityQueue.size
      3

      iex> [{1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.delete_min! |> PriorityQueue.delete_min! |> PriorityQueue.size
      ** (PriorityQueue.EmptyError) queue empty error
  """
  @spec delete_min!(t) :: t | no_return
  def delete_min!(pq) do
    case empty?(pq) do
      true -> raise PriorityQueue.EmptyError
      _    -> delete_min(pq)
    end
  end

  @doc """
  True iff argument is an empty priority queue

      iex> PriorityQueue.new |> PriorityQueue.empty?
      true

      iex> [{4, []} ,{8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.empty?
      false
  """
  @spec empty?(t) :: boolean
  def empty?(%__MODULE__{heap: nil, key_set: %MapSet{map: set}}) when map_size(set) == 0, do: true
  def empty?(_), do: false

  @doc """
  Merge two priority queues

      iex> PriorityQueue.merge( Enum.into([{4, []}, {8, []}], PriorityQueue.new), Enum.into([{3, []}, {1, ["first"]}], PriorityQueue.new)) |> PriorityQueue.to_list
      [{1, ["first"]}, {3, []}, {4, []}, {8, []}]
  """
  @spec merge(t, t) :: t
  def merge(pq = %__MODULE__{heap: heap0, key_set: set0}, %__MODULE__{heap: heap1, key_set: set1}) do
    %{pq | heap: @heap.meld(heap0, heap1), key_set: MapSet.union(set0, set1)}
  end

  @doc """
  Return the minimum element

  If the queue is empty, returns the default value ({nil, nil} if no default value)

      iex> [{4, []} ,{8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.min
      {1, ["first"]}

      iex> PriorityQueue.new |> PriorityQueue.min({:empty, nil})
      {:empty, nil}
  """
  @spec min(t, element) :: element
  def min(%__MODULE__{heap: heap}, default \\ {nil, nil}), do: @heap.min(heap, default)

  @doc """
  Return the minimum element

  Raises a PriorityQueue.EmptyError exception if the queue is empty

      iex> [{4, []} ,{8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.min!
      {1, ["first"]}

      iex> PriorityQueue.new |> PriorityQueue.min!
      ** (PriorityQueue.EmptyError) queue empty error
  """
  @spec min!(t) :: element | no_return
  def min!(pq) do
    case empty?(pq) do
      true -> raise PriorityQueue.EmptyError
      _    -> __MODULE__.min(pq)
    end
  end

  @doc """
  Returns new, empty priority queue
  """
  def new, do: %__MODULE__{}

  @doc """
  Returns the min item, as well as the queue without the min item

  If the queue is empty, returns the default value ({nil, nil} if no default value)

  Equivalent to:
    {min(pq), delete_min(pq)}

      iex> [{4, []} ,{8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.pop |> elem(0)
      {1, ["first"]}

      iex> [{4, []} ,{8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.pop |> elem(1) |> PriorityQueue.to_list
      [{3, []}, {4, []}, {8, []}]

      iex> [{1, "first"}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.pop |> elem(1) |> PriorityQueue.pop({:empty, nil}) |> elem(0)
      {:empty, nil}

      iex> PriorityQueue.new |> PriorityQueue.pop({:empty, nil}) |> elem(0)
      {:empty, nil}
  """
  @spec pop(t, element) :: {element, t}
  def pop(pq = %__MODULE__{heap: heap, key_set: set}, default \\ {nil, nil}) do
    case empty?(pq) do
      true -> {default, pq}
      _    -> {{k, _v} = e, heap} = @heap.pop(heap, default)
              {e, %{pq | heap: heap, key_set: MapSet.delete(set, k)}}
    end
  end

  @doc """
  Returns the min item, as well as the queue without the min item

  Raises a PriorityQueue.EmptyError exception if the queue is empty

  Equivalent to:
    {min!(pq), delete_min!(pq)}

      iex> [{4, []} ,{8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.pop! |> elem(0)
      {1, ["first"]}

      iex> [{1, "first"}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.pop! |> elem(1) |> PriorityQueue.pop! |> elem(0)
      ** (PriorityQueue.EmptyError) queue empty error

      iex> PriorityQueue.new |> PriorityQueue.pop! |> elem(0)
      ** (PriorityQueue.EmptyError) queue empty error
  """
  @spec pop!(t) :: {element, t} | no_return
  def pop!(pq) do
    case empty?(pq) do
      true -> raise PriorityQueue.EmptyError
      _    -> pop(pq)
    end
  end

  @doc """
  Add (insert) element key,value to priority queue
  Pass key/value either as two arguments, or as a tuple {key,value}

      iex> PriorityQueue.new |> PriorityQueue.put(1) |> PriorityQueue.to_list
      [{1, nil}]

      iex> PriorityQueue.new |> PriorityQueue.put(1, "first") |> PriorityQueue.to_list
      [{1, "first"}]

      iex> PriorityQueue.new |> PriorityQueue.put({1, "first"}) |> PriorityQueue.to_list
      [{1, "first"}]
  """
  @spec put(t, {key, value}) :: t
  @spec put(t, key, value | none) :: t
  def put(pq = %__MODULE__{heap: heap, key_set: set}, {key, value}) do
    %{pq | heap: @heap.put(heap, key, value), key_set: MapSet.put(set, key)}
  end
  def put(pq = %__MODULE__{heap: heap, key_set: set}, key, value \\ nil) do
    %{pq | heap: @heap.put(heap, key, value), key_set: MapSet.put(set, key)}
  end

  @doc """
  Number of elements in queue

      iex> [{4, []} ,{8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.size
      4
  """
  @spec size(t) :: non_neg_integer
  def size(%__MODULE__{key_set: map}), do: MapSet.size(map)

  @doc """
  Retrieve elements from priority queue as a list in sorted order

      iex> [{4, []}, {8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.to_list
      [{1, ["first"]}, {3, []}, {4, []}, {8, []}]
  """
  @spec to_list(t) :: list
  def to_list(%__MODULE__{heap: nil}), do: []
  def to_list(pq) do
    [__MODULE__.min(pq) | to_list(delete_min(pq))]
  end

  @doc """
  Retrieve keys from priority queue as a list in sorted order (may have duplicates)

  Heap sort a list
      iex> [{4, []}, {8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.keys
      [1, 3, 4, 8]
  """
  @spec keys(t) :: list
  def keys(%__MODULE__{heap: nil}), do: []
  def keys(%__MODULE__{key_set: set}) do
    MapSet.to_list(set)
  end

  @doc """
  Retrieve values from priority queue as a list, sorted in order of their keys

      iex> [{4, []}, {8, ["last"]}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.values
      [["first"], [], [], ["last"]]
  """
  @spec values(t) :: list
  def values(%__MODULE__{heap: nil}), do: []
  def values(pq) do
    {_k, v} = min(pq)
    [v | values(delete_min(pq))]
  end

end


defimpl Collectable, for: PriorityQueue do

  def empty(_pq) do
    PriorityQueue.new
  end

  @doc """
  Implements 'into' for PriorityQueue

      iex> [{4, []}, {8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> PriorityQueue.size
      4
  """
  def into(original) do
    # {original, fn
    #   pq, {:cont, {k, v}} -> PriorityQueue.put(pq, k, v)
    #   pq, {:cont, {k}} -> PriorityQueue.put(pq, k, nil)
    #   pq, {:cont, k} -> PriorityQueue.put(pq, k, nil)
    #   pq, :done -> pq
    #   _, :halt -> :ok
    # end}

    collector_fun = fn
      pq, {:cont, {k, v}} -> PriorityQueue.put(pq, k, v)
      # pq, {:cont, {k}} -> PriorityQueue.put(pq, k, nil)
      # pq, {:cont, k} -> PriorityQueue.put(pq, k, nil)
      pq, :done -> pq
      _pq, :halt -> :ok
    end

    {original, collector_fun}
  end
end


defimpl Enumerable, for: PriorityQueue do

  @doc """
  Implements 'reduce' for PriorityQueue

  Currently traverses in priority order. This may change in the future.
  DO NOT RELY ON TRAVERSAL ORDER

      iex> [{4, []}, {8, []}, {3, []}, {1, ["first"]}] |> Enum.into(PriorityQueue.new) |> Enum.reduce(0, &(elem(&1,0) + &2))
      16
  """
  def reduce(_,   {:halt, acc}, _fun),    do: {:halted, acc}
  def reduce(pq,  {:suspend, acc}, fun),  do: {:suspended, acc, &reduce(pq, &1, fun)}
  def reduce(pq,  {:cont, acc}, fun)      do
    cond do
      PriorityQueue.empty?(pq) -> {:done, acc}
      true                     -> {e, pq} = PriorityQueue.pop(pq);
                                  reduce(pq, fun.(e, acc), fun)
    end
  end

  @doc """
  Implements 'member?' for PriorityQueue

  Uses knowledge that we traverse in sorted order

      iex> [4,{8},3,{1, "first"}] |> Enum.into(PriorityQueue.new) |> Enum.member?({4,nil})
      true
  """
  def member?(pq, e = {k, _v}) do
    if PriorityQueue.empty?(pq) do
      {:ok, false}
    else
      {e_h = {k_h, _}, pq} = PriorityQueue.pop(pq)
      cond do
        k_h > k   -> {:ok, false}
        e === e_h -> {:ok, true}
        true      -> member?(pq, e)
      end
    end
  end

  @doc """
  Implements 'count' for PriorityQueue

      iex> Enum.into([4,{8},3,{1, "first"}], PriorityQueue.new) |> Enum.count
      4
  """
  def count(pq), do: {:ok, PriorityQueue.size(pq)}
end
