Initial declaration audit failed before writing its result: Counter.update
received the mapping of match names to AST paths, instead of its keys.
TypeError: can only concatenate list (not "int") to list.
The original script is source.py. No query, native loading or timing occurred.
The corrected audit counts matching requests using matches.keys().
