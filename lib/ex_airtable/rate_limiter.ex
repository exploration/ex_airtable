defmodule RateLimiter do
  use GenStage

  alias ExAirtable.RateLimiter.{Producer, Request}

  @doc """
  State is a map of %Producer{} structs where the key is the module name of the producer.
  """
  def init(_) do
    {:consumer, %{}}
  end

  def handle_subscribe(:producer, _opts, from, producers) do
    producers = Map.put(producers, from, %Producer{})
    producers = ask_and_schedule(producers, from)

    # Returns :manual, as we want control over the demand
    {:manual, producers}
  end

  def handle_cancel(_, from, producers) do
    {:noreply, [], Map.delete(producers, from)}
  end

  def handle_events(events, _from, producers) do
    Enum.each(events, &Request.run/1)

    {:noreply, [], producers}
  end

  def handle_info({:ask, from}, producers) do
    {:noreply, [], ask_and_schedule(producers, from)}
  end

  defp ask_and_schedule(producers, from) do
    case producers do
      %{^from => %Producer{} = producer} ->
        GenStage.ask(from, producer.max_requests)
        Process.send_after(self(), {:ask, from}, producer.interval)
      %{} ->
        producers
    end
  end
end