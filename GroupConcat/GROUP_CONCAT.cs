/*
GROUP_CONCAT string aggregate for SQL Server - https://groupconcat.codeplex.com
Copyright (C) 2011  Orlando Colamatteo

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or 
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

See http://www.gnu.org/licenses/ for a copy of the GNU General Public 
License.
*/

using System;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.IO;
using System.Collections.Generic;
using System.Text;

namespace GroupConcat
{
  [Serializable]
  [SqlUserDefinedAggregate(Format.UserDefined,
                           MaxByteSize = -1,
                           IsInvariantToNulls = true,
                           IsInvariantToDuplicates = false,
                           IsInvariantToOrder = true,
                           IsNullIfEmpty = true)]
  // ReSharper disable once InconsistentNaming
  public struct GROUP_CONCAT : IBinarySerialize
  {
    private Dictionary<string, int> _values;

    public void Init()
    {
      _values = new Dictionary<string, int>();
    }

    public void Accumulate([SqlFacet(MaxSize = 4000)] SqlString value)
    {
      if (!value.IsNull)
      {
        string key = value.Value;
        if (_values.ContainsKey(key))
        {
          _values[key] += 1;
        }
        else
        {
          _values.Add(key, 1);
        }
      }
    }

    public void Merge(GROUP_CONCAT group)
    {
      foreach (KeyValuePair<string, int> item in group._values)
      {
        string key = item.Key;
        if (_values.ContainsKey(key))
        {
          _values[key] += group._values[key];
        }
        else
        {
          _values.Add(key, group._values[key]);
        }
      }
    }

    [return: SqlFacet(MaxSize = -1)]
    public SqlString Terminate()
    {
      if (_values != null && _values.Count > 0)
      {
        StringBuilder returnStringBuilder = new StringBuilder();

        foreach (KeyValuePair<string, int> item in _values)
        {
          for (int value = 0; value < item.Value; value++)
          {
            returnStringBuilder.Append(item.Key);
            returnStringBuilder.Append(',');
          }
        }
        return returnStringBuilder.Remove(returnStringBuilder.Length - 1, 1).ToString();
      }

      return null;
    }

    public void Read(BinaryReader r)
    {
      int itemCount = r.ReadInt32();
      _values = new Dictionary<string, int>(itemCount);
      for (int i = 0; i <= itemCount - 1; i++)
      {
        _values.Add(r.ReadString(), r.ReadInt32());
      }
    }

    public void Write(BinaryWriter w)
    {
      w.Write(_values.Count);
      foreach (KeyValuePair<string, int> s in _values)
      {
        w.Write(s.Key);
        w.Write(s.Value);
      }
    }
  }
}
