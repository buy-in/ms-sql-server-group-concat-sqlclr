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

using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
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
  public struct GROUP_CONCAT_S : IBinarySerialize
  {
    private Dictionary<string, int> _values;
    private byte _sortBy;

    private SqlByte SortBy
    {
      set
      {
        if (_sortBy == 0)
        {
          if (
              value.Value != 1 // ASC
              &&
              value.Value != 2 // DESC
              )
          {
            throw new Exception("Invalid SortBy value: use 1 for ASC or 2 for DESC.");
          }
          _sortBy = Convert.ToByte(value.Value);
        }
      }
    }

    public void Init()
    {
      _values = new Dictionary<string, int>();
      _sortBy = 0;
    }

    public void Accumulate([SqlFacet(MaxSize = 4000)] SqlString value,
                           SqlByte sortOrder)
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
        SortBy = sortOrder;
      }
    }

    public void Merge(GROUP_CONCAT_S group)
    {
      if (_sortBy == 0)
      {
        _sortBy = group._sortBy;
      }

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
        var returnStringBuilder = new StringBuilder();

        var sortedValues = _sortBy == 2 
          ? new SortedDictionary<string, int>(_values, new ReverseComparer()) 
          : new SortedDictionary<string, int>(_values);

        // iterate over the SortedDictionary
        foreach (KeyValuePair<string, int> item in sortedValues)
        {
          string key = item.Key;
          for (int value = 0; value < item.Value; value++)
          {
            returnStringBuilder.Append(key);
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
      _sortBy = r.ReadByte();
    }

    public void Write(BinaryWriter w)
    {
      w.Write(this._values.Count);
      foreach (KeyValuePair<string, int> s in _values)
      {
        w.Write(s.Key);
        w.Write(s.Value);
      }
      w.Write(_sortBy);
    }
  }
}
