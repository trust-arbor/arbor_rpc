root = Path.expand("..", __DIR__)
sources = Path.wildcard(Path.join(root, "lib/**/*.ex"))
if sources == [], do: raise("RPC production source is missing")

violations =
  Enum.flat_map(sources, fn path ->
    ast = path |> File.read!() |> Code.string_to_quoted!()

    {_ast, references} =
      Macro.prewalk(ast, [], fn
        {:__aliases__, metadata, parts} = node, acc ->
          {node, [{Enum.map_join(parts, ".", &Atom.to_string/1), metadata[:line]} | acc]}

        node, acc ->
          {node, acc}
      end)

    for {module, line} <- references,
        String.starts_with?(module, ["ExMCP", "ExACP", "Arbor.ACP", "Arbor.MCP"]) do
      "#{Path.relative_to(path, root)}:#{line}: #{module}"
    end
  end)

if violations == [] do
  IO.puts("RPC source boundaries pass")
else
  Enum.each(violations, &IO.puts/1)
  System.halt(1)
end
