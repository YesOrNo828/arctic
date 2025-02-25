/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.netease.arctic.ams.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2022-06-30")
public class TableCommitMeta implements org.apache.thrift.TBase<TableCommitMeta, TableCommitMeta._Fields>, java.io.Serializable, Cloneable, Comparable<TableCommitMeta> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TableCommitMeta");

  private static final org.apache.thrift.protocol.TField TABLE_IDENTIFIER_FIELD_DESC = new org.apache.thrift.protocol.TField("tableIdentifier", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField ACTION_FIELD_DESC = new org.apache.thrift.protocol.TField("action", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField CHANGES_FIELD_DESC = new org.apache.thrift.protocol.TField("changes", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField COMMIT_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("commitTime", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField PROPERTIES_FIELD_DESC = new org.apache.thrift.protocol.TField("properties", org.apache.thrift.protocol.TType.MAP, (short)5);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TableCommitMetaStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TableCommitMetaTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable com.netease.arctic.ams.api.TableIdentifier tableIdentifier; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String action; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<TableChange> changes; // required
  public long commitTime; // required
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> properties; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TABLE_IDENTIFIER((short)1, "tableIdentifier"),
    ACTION((short)2, "action"),
    CHANGES((short)3, "changes"),
    COMMIT_TIME((short)4, "commitTime"),
    PROPERTIES((short)5, "properties");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TABLE_IDENTIFIER
          return TABLE_IDENTIFIER;
        case 2: // ACTION
          return ACTION;
        case 3: // CHANGES
          return CHANGES;
        case 4: // COMMIT_TIME
          return COMMIT_TIME;
        case 5: // PROPERTIES
          return PROPERTIES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __COMMITTIME_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TABLE_IDENTIFIER, new org.apache.thrift.meta_data.FieldMetaData("tableIdentifier", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, com.netease.arctic.ams.api.TableIdentifier.class)));
    tmpMap.put(_Fields.ACTION, new org.apache.thrift.meta_data.FieldMetaData("action", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CHANGES, new org.apache.thrift.meta_data.FieldMetaData("changes", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TableChange.class))));
    tmpMap.put(_Fields.COMMIT_TIME, new org.apache.thrift.meta_data.FieldMetaData("commitTime", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.PROPERTIES, new org.apache.thrift.meta_data.FieldMetaData("properties", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TableCommitMeta.class, metaDataMap);
  }

  public TableCommitMeta() {
  }

  public TableCommitMeta(
    com.netease.arctic.ams.api.TableIdentifier tableIdentifier,
    java.lang.String action,
    java.util.List<TableChange> changes,
    long commitTime,
    java.util.Map<java.lang.String,java.lang.String> properties)
  {
    this();
    this.tableIdentifier = tableIdentifier;
    this.action = action;
    this.changes = changes;
    this.commitTime = commitTime;
    setCommitTimeIsSet(true);
    this.properties = properties;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TableCommitMeta(TableCommitMeta other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetTableIdentifier()) {
      this.tableIdentifier = new com.netease.arctic.ams.api.TableIdentifier(other.tableIdentifier);
    }
    if (other.isSetAction()) {
      this.action = other.action;
    }
    if (other.isSetChanges()) {
      java.util.List<TableChange> __this__changes = new java.util.ArrayList<TableChange>(other.changes.size());
      for (TableChange other_element : other.changes) {
        __this__changes.add(new TableChange(other_element));
      }
      this.changes = __this__changes;
    }
    this.commitTime = other.commitTime;
    if (other.isSetProperties()) {
      java.util.Map<java.lang.String,java.lang.String> __this__properties = new java.util.HashMap<java.lang.String,java.lang.String>(other.properties);
      this.properties = __this__properties;
    }
  }

  public TableCommitMeta deepCopy() {
    return new TableCommitMeta(this);
  }

  @Override
  public void clear() {
    this.tableIdentifier = null;
    this.action = null;
    this.changes = null;
    setCommitTimeIsSet(false);
    this.commitTime = 0;
    this.properties = null;
  }

  @org.apache.thrift.annotation.Nullable
  public com.netease.arctic.ams.api.TableIdentifier getTableIdentifier() {
    return this.tableIdentifier;
  }

  public TableCommitMeta setTableIdentifier(@org.apache.thrift.annotation.Nullable com.netease.arctic.ams.api.TableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
    return this;
  }

  public void unsetTableIdentifier() {
    this.tableIdentifier = null;
  }

  /** Returns true if field tableIdentifier is set (has been assigned a value) and false otherwise */
  public boolean isSetTableIdentifier() {
    return this.tableIdentifier != null;
  }

  public void setTableIdentifierIsSet(boolean value) {
    if (!value) {
      this.tableIdentifier = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getAction() {
    return this.action;
  }

  public TableCommitMeta setAction(@org.apache.thrift.annotation.Nullable java.lang.String action) {
    this.action = action;
    return this;
  }

  public void unsetAction() {
    this.action = null;
  }

  /** Returns true if field action is set (has been assigned a value) and false otherwise */
  public boolean isSetAction() {
    return this.action != null;
  }

  public void setActionIsSet(boolean value) {
    if (!value) {
      this.action = null;
    }
  }

  public int getChangesSize() {
    return (this.changes == null) ? 0 : this.changes.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<TableChange> getChangesIterator() {
    return (this.changes == null) ? null : this.changes.iterator();
  }

  public void addToChanges(TableChange elem) {
    if (this.changes == null) {
      this.changes = new java.util.ArrayList<TableChange>();
    }
    this.changes.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<TableChange> getChanges() {
    return this.changes;
  }

  public TableCommitMeta setChanges(@org.apache.thrift.annotation.Nullable java.util.List<TableChange> changes) {
    this.changes = changes;
    return this;
  }

  public void unsetChanges() {
    this.changes = null;
  }

  /** Returns true if field changes is set (has been assigned a value) and false otherwise */
  public boolean isSetChanges() {
    return this.changes != null;
  }

  public void setChangesIsSet(boolean value) {
    if (!value) {
      this.changes = null;
    }
  }

  public long getCommitTime() {
    return this.commitTime;
  }

  public TableCommitMeta setCommitTime(long commitTime) {
    this.commitTime = commitTime;
    setCommitTimeIsSet(true);
    return this;
  }

  public void unsetCommitTime() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __COMMITTIME_ISSET_ID);
  }

  /** Returns true if field commitTime is set (has been assigned a value) and false otherwise */
  public boolean isSetCommitTime() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __COMMITTIME_ISSET_ID);
  }

  public void setCommitTimeIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __COMMITTIME_ISSET_ID, value);
  }

  public int getPropertiesSize() {
    return (this.properties == null) ? 0 : this.properties.size();
  }

  public void putToProperties(java.lang.String key, java.lang.String val) {
    if (this.properties == null) {
      this.properties = new java.util.HashMap<java.lang.String,java.lang.String>();
    }
    this.properties.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,java.lang.String> getProperties() {
    return this.properties;
  }

  public TableCommitMeta setProperties(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> properties) {
    this.properties = properties;
    return this;
  }

  public void unsetProperties() {
    this.properties = null;
  }

  /** Returns true if field properties is set (has been assigned a value) and false otherwise */
  public boolean isSetProperties() {
    return this.properties != null;
  }

  public void setPropertiesIsSet(boolean value) {
    if (!value) {
      this.properties = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case TABLE_IDENTIFIER:
      if (value == null) {
        unsetTableIdentifier();
      } else {
        setTableIdentifier((com.netease.arctic.ams.api.TableIdentifier)value);
      }
      break;

    case ACTION:
      if (value == null) {
        unsetAction();
      } else {
        setAction((java.lang.String)value);
      }
      break;

    case CHANGES:
      if (value == null) {
        unsetChanges();
      } else {
        setChanges((java.util.List<TableChange>)value);
      }
      break;

    case COMMIT_TIME:
      if (value == null) {
        unsetCommitTime();
      } else {
        setCommitTime((java.lang.Long)value);
      }
      break;

    case PROPERTIES:
      if (value == null) {
        unsetProperties();
      } else {
        setProperties((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case TABLE_IDENTIFIER:
      return getTableIdentifier();

    case ACTION:
      return getAction();

    case CHANGES:
      return getChanges();

    case COMMIT_TIME:
      return getCommitTime();

    case PROPERTIES:
      return getProperties();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case TABLE_IDENTIFIER:
      return isSetTableIdentifier();
    case ACTION:
      return isSetAction();
    case CHANGES:
      return isSetChanges();
    case COMMIT_TIME:
      return isSetCommitTime();
    case PROPERTIES:
      return isSetProperties();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof TableCommitMeta)
      return this.equals((TableCommitMeta)that);
    return false;
  }

  public boolean equals(TableCommitMeta that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_tableIdentifier = true && this.isSetTableIdentifier();
    boolean that_present_tableIdentifier = true && that.isSetTableIdentifier();
    if (this_present_tableIdentifier || that_present_tableIdentifier) {
      if (!(this_present_tableIdentifier && that_present_tableIdentifier))
        return false;
      if (!this.tableIdentifier.equals(that.tableIdentifier))
        return false;
    }

    boolean this_present_action = true && this.isSetAction();
    boolean that_present_action = true && that.isSetAction();
    if (this_present_action || that_present_action) {
      if (!(this_present_action && that_present_action))
        return false;
      if (!this.action.equals(that.action))
        return false;
    }

    boolean this_present_changes = true && this.isSetChanges();
    boolean that_present_changes = true && that.isSetChanges();
    if (this_present_changes || that_present_changes) {
      if (!(this_present_changes && that_present_changes))
        return false;
      if (!this.changes.equals(that.changes))
        return false;
    }

    boolean this_present_commitTime = true;
    boolean that_present_commitTime = true;
    if (this_present_commitTime || that_present_commitTime) {
      if (!(this_present_commitTime && that_present_commitTime))
        return false;
      if (this.commitTime != that.commitTime)
        return false;
    }

    boolean this_present_properties = true && this.isSetProperties();
    boolean that_present_properties = true && that.isSetProperties();
    if (this_present_properties || that_present_properties) {
      if (!(this_present_properties && that_present_properties))
        return false;
      if (!this.properties.equals(that.properties))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetTableIdentifier()) ? 131071 : 524287);
    if (isSetTableIdentifier())
      hashCode = hashCode * 8191 + tableIdentifier.hashCode();

    hashCode = hashCode * 8191 + ((isSetAction()) ? 131071 : 524287);
    if (isSetAction())
      hashCode = hashCode * 8191 + action.hashCode();

    hashCode = hashCode * 8191 + ((isSetChanges()) ? 131071 : 524287);
    if (isSetChanges())
      hashCode = hashCode * 8191 + changes.hashCode();

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(commitTime);

    hashCode = hashCode * 8191 + ((isSetProperties()) ? 131071 : 524287);
    if (isSetProperties())
      hashCode = hashCode * 8191 + properties.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TableCommitMeta other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetTableIdentifier()).compareTo(other.isSetTableIdentifier());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableIdentifier()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableIdentifier, other.tableIdentifier);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetAction()).compareTo(other.isSetAction());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAction()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.action, other.action);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetChanges()).compareTo(other.isSetChanges());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetChanges()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.changes, other.changes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetCommitTime()).compareTo(other.isSetCommitTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCommitTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.commitTime, other.commitTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetProperties()).compareTo(other.isSetProperties());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetProperties()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.properties, other.properties);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TableCommitMeta(");
    boolean first = true;

    sb.append("tableIdentifier:");
    if (this.tableIdentifier == null) {
      sb.append("null");
    } else {
      sb.append(this.tableIdentifier);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("action:");
    if (this.action == null) {
      sb.append("null");
    } else {
      sb.append(this.action);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("changes:");
    if (this.changes == null) {
      sb.append("null");
    } else {
      sb.append(this.changes);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("commitTime:");
    sb.append(this.commitTime);
    first = false;
    if (!first) sb.append(", ");
    sb.append("properties:");
    if (this.properties == null) {
      sb.append("null");
    } else {
      sb.append(this.properties);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (tableIdentifier != null) {
      tableIdentifier.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TableCommitMetaStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TableCommitMetaStandardScheme getScheme() {
      return new TableCommitMetaStandardScheme();
    }
  }

  private static class TableCommitMetaStandardScheme extends org.apache.thrift.scheme.StandardScheme<TableCommitMeta> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TableCommitMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TABLE_IDENTIFIER
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.tableIdentifier = new com.netease.arctic.ams.api.TableIdentifier();
              struct.tableIdentifier.read(iprot);
              struct.setTableIdentifierIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ACTION
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.action = iprot.readString();
              struct.setActionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CHANGES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list64 = iprot.readListBegin();
                struct.changes = new java.util.ArrayList<TableChange>(_list64.size);
                @org.apache.thrift.annotation.Nullable TableChange _elem65;
                for (int _i66 = 0; _i66 < _list64.size; ++_i66)
                {
                  _elem65 = new TableChange();
                  _elem65.read(iprot);
                  struct.changes.add(_elem65);
                }
                iprot.readListEnd();
              }
              struct.setChangesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // COMMIT_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.commitTime = iprot.readI64();
              struct.setCommitTimeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // PROPERTIES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map67 = iprot.readMapBegin();
                struct.properties = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map67.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key68;
                @org.apache.thrift.annotation.Nullable java.lang.String _val69;
                for (int _i70 = 0; _i70 < _map67.size; ++_i70)
                {
                  _key68 = iprot.readString();
                  _val69 = iprot.readString();
                  struct.properties.put(_key68, _val69);
                }
                iprot.readMapEnd();
              }
              struct.setPropertiesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TableCommitMeta struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tableIdentifier != null) {
        oprot.writeFieldBegin(TABLE_IDENTIFIER_FIELD_DESC);
        struct.tableIdentifier.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.action != null) {
        oprot.writeFieldBegin(ACTION_FIELD_DESC);
        oprot.writeString(struct.action);
        oprot.writeFieldEnd();
      }
      if (struct.changes != null) {
        oprot.writeFieldBegin(CHANGES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.changes.size()));
          for (TableChange _iter71 : struct.changes)
          {
            _iter71.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(COMMIT_TIME_FIELD_DESC);
      oprot.writeI64(struct.commitTime);
      oprot.writeFieldEnd();
      if (struct.properties != null) {
        oprot.writeFieldBegin(PROPERTIES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.properties.size()));
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter72 : struct.properties.entrySet())
          {
            oprot.writeString(_iter72.getKey());
            oprot.writeString(_iter72.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TableCommitMetaTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TableCommitMetaTupleScheme getScheme() {
      return new TableCommitMetaTupleScheme();
    }
  }

  private static class TableCommitMetaTupleScheme extends org.apache.thrift.scheme.TupleScheme<TableCommitMeta> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TableCommitMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetTableIdentifier()) {
        optionals.set(0);
      }
      if (struct.isSetAction()) {
        optionals.set(1);
      }
      if (struct.isSetChanges()) {
        optionals.set(2);
      }
      if (struct.isSetCommitTime()) {
        optionals.set(3);
      }
      if (struct.isSetProperties()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetTableIdentifier()) {
        struct.tableIdentifier.write(oprot);
      }
      if (struct.isSetAction()) {
        oprot.writeString(struct.action);
      }
      if (struct.isSetChanges()) {
        {
          oprot.writeI32(struct.changes.size());
          for (TableChange _iter73 : struct.changes)
          {
            _iter73.write(oprot);
          }
        }
      }
      if (struct.isSetCommitTime()) {
        oprot.writeI64(struct.commitTime);
      }
      if (struct.isSetProperties()) {
        {
          oprot.writeI32(struct.properties.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter74 : struct.properties.entrySet())
          {
            oprot.writeString(_iter74.getKey());
            oprot.writeString(_iter74.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TableCommitMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.tableIdentifier = new com.netease.arctic.ams.api.TableIdentifier();
        struct.tableIdentifier.read(iprot);
        struct.setTableIdentifierIsSet(true);
      }
      if (incoming.get(1)) {
        struct.action = iprot.readString();
        struct.setActionIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list75 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.changes = new java.util.ArrayList<TableChange>(_list75.size);
          @org.apache.thrift.annotation.Nullable TableChange _elem76;
          for (int _i77 = 0; _i77 < _list75.size; ++_i77)
          {
            _elem76 = new TableChange();
            _elem76.read(iprot);
            struct.changes.add(_elem76);
          }
        }
        struct.setChangesIsSet(true);
      }
      if (incoming.get(3)) {
        struct.commitTime = iprot.readI64();
        struct.setCommitTimeIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TMap _map78 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.properties = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map78.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key79;
          @org.apache.thrift.annotation.Nullable java.lang.String _val80;
          for (int _i81 = 0; _i81 < _map78.size; ++_i81)
          {
            _key79 = iprot.readString();
            _val80 = iprot.readString();
            struct.properties.put(_key79, _val80);
          }
        }
        struct.setPropertiesIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

