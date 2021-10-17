import json

def JSONIterator(fin):
  fin.seek(0, 2)
  file_size = fin.tell()
  fin.seek(1)
  while file_size > fin.tell() + 2:
    brace_count = 0
    in_quote = False
    escaping = False
    chars = [fin.read(1), fin.read(1)]
    for i in range(0, 2):
      if (chars[i] == '{'):
        brace_count += 1
      if (chars[i] == '"'):
        in_quote = not in_quote
      if escaping:
        escaping = False
      else:
        if (chars[i] == '\\'):
          escaping = True
    while(chars[-1] != ',' or brace_count != 0):
      c = fin.read(1)
      if (c):
        chars.append(c)
        if c == '"' and not escaping:
          in_quote = not in_quote
        else:
          if (not in_quote):
            if (c == '{'):
              brace_count += 1
            elif (c == '}'):
              brace_count -= 1
        if escaping:
          escaping = False
        else:
          if (c == '\\'):
            escaping = True
      else:
        break
    yield ''.join(chars[:-1])