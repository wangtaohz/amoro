/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.netease.arctic.ams.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2023-05-06")
public class TableMeta implements org.apache.thrift.TBase<TableMeta, TableMeta._Fields>, java.io.Serializable, Cloneable, Comparable<TableMeta> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TableMeta");

  private static final org.apache.thrift.protocol.TField TABLE_IDENTIFIER_FIELD_DESC = new org.apache.thrift.protocol.TField("tableIdentifier", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField KEY_SPEC_FIELD_DESC = new org.apache.thrift.protocol.TField("keySpec", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField PROPERTIES_FIELD_DESC = new org.apache.thrift.protocol.TField("properties", org.apache.thrift.protocol.TType.MAP, (short)3);
  private static final org.apache.thrift.protocol.TField LOCATIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("locations", org.apache.thrift.protocol.TType.MAP, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TableMetaStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TableMetaTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable com.netease.arctic.ams.api.TableIdentifier tableIdentifier; // required
  public @org.apache.thrift.annotation.Nullable PrimaryKeySpec keySpec; // required
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> properties; // optional
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> locations; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TABLE_IDENTIFIER((short)1, "tableIdentifier"),
    KEY_SPEC((short)2, "keySpec"),
    PROPERTIES((short)3, "properties"),
    LOCATIONS((short)4, "locations");

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
        case 2: // KEY_SPEC
          return KEY_SPEC;
        case 3: // PROPERTIES
          return PROPERTIES;
        case 4: // LOCATIONS
          return LOCATIONS;
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
  private static final _Fields optionals[] = {_Fields.PROPERTIES};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TABLE_IDENTIFIER, new org.apache.thrift.meta_data.FieldMetaData("tableIdentifier", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, com.netease.arctic.ams.api.TableIdentifier.class)));
    tmpMap.put(_Fields.KEY_SPEC, new org.apache.thrift.meta_data.FieldMetaData("keySpec", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT        , "PrimaryKeySpec")));
    tmpMap.put(_Fields.PROPERTIES, new org.apache.thrift.meta_data.FieldMetaData("properties", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.LOCATIONS, new org.apache.thrift.meta_data.FieldMetaData("locations", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TableMeta.class, metaDataMap);
  }

  public TableMeta() {
  }

  public TableMeta(
    com.netease.arctic.ams.api.TableIdentifier tableIdentifier,
    PrimaryKeySpec keySpec,
    java.util.Map<java.lang.String,java.lang.String> locations)
  {
    this();
    this.tableIdentifier = tableIdentifier;
    this.keySpec = keySpec;
    this.locations = locations;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TableMeta(TableMeta other) {
    if (other.isSetTableIdentifier()) {
      this.tableIdentifier = new com.netease.arctic.ams.api.TableIdentifier(other.tableIdentifier);
    }
    if (other.isSetKeySpec()) {
      this.keySpec = new PrimaryKeySpec(other.keySpec);
    }
    if (other.isSetProperties()) {
      java.util.Map<java.lang.String,java.lang.String> __this__properties = new java.util.HashMap<java.lang.String,java.lang.String>(other.properties);
      this.properties = __this__properties;
    }
    if (other.isSetLocations()) {
      java.util.Map<java.lang.String,java.lang.String> __this__locations = new java.util.HashMap<java.lang.String,java.lang.String>(other.locations);
      this.locations = __this__locations;
    }
  }

  public TableMeta deepCopy() {
    return new TableMeta(this);
  }

  @Override
  public void clear() {
    this.tableIdentifier = null;
    this.keySpec = null;
    this.properties = null;
    this.locations = null;
  }

  @org.apache.thrift.annotation.Nullable
  public com.netease.arctic.ams.api.TableIdentifier getTableIdentifier() {
    return this.tableIdentifier;
  }

  public TableMeta setTableIdentifier(@org.apache.thrift.annotation.Nullable com.netease.arctic.ams.api.TableIdentifier tableIdentifier) {
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
  public PrimaryKeySpec getKeySpec() {
    return this.keySpec;
  }

  public TableMeta setKeySpec(@org.apache.thrift.annotation.Nullable PrimaryKeySpec keySpec) {
    this.keySpec = keySpec;
    return this;
  }

  public void unsetKeySpec() {
    this.keySpec = null;
  }

  /** Returns true if field keySpec is set (has been assigned a value) and false otherwise */
  public boolean isSetKeySpec() {
    return this.keySpec != null;
  }

  public void setKeySpecIsSet(boolean value) {
    if (!value) {
      this.keySpec = null;
    }
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

  public TableMeta setProperties(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> properties) {
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

  public int getLocationsSize() {
    return (this.locations == null) ? 0 : this.locations.size();
  }

  public void putToLocations(java.lang.String key, java.lang.String val) {
    if (this.locations == null) {
      this.locations = new java.util.HashMap<java.lang.String,java.lang.String>();
    }
    this.locations.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,java.lang.String> getLocations() {
    return this.locations;
  }

  public TableMeta setLocations(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> locations) {
    this.locations = locations;
    return this;
  }

  public void unsetLocations() {
    this.locations = null;
  }

  /** Returns true if field locations is set (has been assigned a value) and false otherwise */
  public boolean isSetLocations() {
    return this.locations != null;
  }

  public void setLocationsIsSet(boolean value) {
    if (!value) {
      this.locations = null;
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

    case KEY_SPEC:
      if (value == null) {
        unsetKeySpec();
      } else {
        setKeySpec((PrimaryKeySpec)value);
      }
      break;

    case PROPERTIES:
      if (value == null) {
        unsetProperties();
      } else {
        setProperties((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    case LOCATIONS:
      if (value == null) {
        unsetLocations();
      } else {
        setLocations((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case TABLE_IDENTIFIER:
      return getTableIdentifier();

    case KEY_SPEC:
      return getKeySpec();

    case PROPERTIES:
      return getProperties();

    case LOCATIONS:
      return getLocations();

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
    case KEY_SPEC:
      return isSetKeySpec();
    case PROPERTIES:
      return isSetProperties();
    case LOCATIONS:
      return isSetLocations();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof TableMeta)
      return this.equals((TableMeta)that);
    return false;
  }

  public boolean equals(TableMeta that) {
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

    boolean this_present_keySpec = true && this.isSetKeySpec();
    boolean that_present_keySpec = true && that.isSetKeySpec();
    if (this_present_keySpec || that_present_keySpec) {
      if (!(this_present_keySpec && that_present_keySpec))
        return false;
      if (!this.keySpec.equals(that.keySpec))
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

    boolean this_present_locations = true && this.isSetLocations();
    boolean that_present_locations = true && that.isSetLocations();
    if (this_present_locations || that_present_locations) {
      if (!(this_present_locations && that_present_locations))
        return false;
      if (!this.locations.equals(that.locations))
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

    hashCode = hashCode * 8191 + ((isSetKeySpec()) ? 131071 : 524287);
    if (isSetKeySpec())
      hashCode = hashCode * 8191 + keySpec.hashCode();

    hashCode = hashCode * 8191 + ((isSetProperties()) ? 131071 : 524287);
    if (isSetProperties())
      hashCode = hashCode * 8191 + properties.hashCode();

    hashCode = hashCode * 8191 + ((isSetLocations()) ? 131071 : 524287);
    if (isSetLocations())
      hashCode = hashCode * 8191 + locations.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TableMeta other) {
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
    lastComparison = java.lang.Boolean.valueOf(isSetKeySpec()).compareTo(other.isSetKeySpec());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKeySpec()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.keySpec, other.keySpec);
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
    lastComparison = java.lang.Boolean.valueOf(isSetLocations()).compareTo(other.isSetLocations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLocations()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.locations, other.locations);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TableMeta(");
    boolean first = true;

    sb.append("tableIdentifier:");
    if (this.tableIdentifier == null) {
      sb.append("null");
    } else {
      sb.append(this.tableIdentifier);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("keySpec:");
    if (this.keySpec == null) {
      sb.append("null");
    } else {
      sb.append(this.keySpec);
    }
    first = false;
    if (isSetProperties()) {
      if (!first) sb.append(", ");
      sb.append("properties:");
      if (this.properties == null) {
        sb.append("null");
      } else {
        sb.append(this.properties);
      }
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("locations:");
    if (this.locations == null) {
      sb.append("null");
    } else {
      sb.append(this.locations);
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TableMetaStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TableMetaStandardScheme getScheme() {
      return new TableMetaStandardScheme();
    }
  }

  private static class TableMetaStandardScheme extends org.apache.thrift.scheme.StandardScheme<TableMeta> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TableMeta struct) throws org.apache.thrift.TException {
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
          case 2: // KEY_SPEC
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.keySpec = new PrimaryKeySpec();
              struct.keySpec.read(iprot);
              struct.setKeySpecIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // PROPERTIES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map90 = iprot.readMapBegin();
                struct.properties = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map90.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key91;
                @org.apache.thrift.annotation.Nullable java.lang.String _val92;
                for (int _i93 = 0; _i93 < _map90.size; ++_i93)
                {
                  _key91 = iprot.readString();
                  _val92 = iprot.readString();
                  struct.properties.put(_key91, _val92);
                }
                iprot.readMapEnd();
              }
              struct.setPropertiesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // LOCATIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map94 = iprot.readMapBegin();
                struct.locations = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map94.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key95;
                @org.apache.thrift.annotation.Nullable java.lang.String _val96;
                for (int _i97 = 0; _i97 < _map94.size; ++_i97)
                {
                  _key95 = iprot.readString();
                  _val96 = iprot.readString();
                  struct.locations.put(_key95, _val96);
                }
                iprot.readMapEnd();
              }
              struct.setLocationsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TableMeta struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tableIdentifier != null) {
        oprot.writeFieldBegin(TABLE_IDENTIFIER_FIELD_DESC);
        struct.tableIdentifier.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.keySpec != null) {
        oprot.writeFieldBegin(KEY_SPEC_FIELD_DESC);
        struct.keySpec.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.properties != null) {
        if (struct.isSetProperties()) {
          oprot.writeFieldBegin(PROPERTIES_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.properties.size()));
            for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter98 : struct.properties.entrySet())
            {
              oprot.writeString(_iter98.getKey());
              oprot.writeString(_iter98.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.locations != null) {
        oprot.writeFieldBegin(LOCATIONS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.locations.size()));
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter99 : struct.locations.entrySet())
          {
            oprot.writeString(_iter99.getKey());
            oprot.writeString(_iter99.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TableMetaTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TableMetaTupleScheme getScheme() {
      return new TableMetaTupleScheme();
    }
  }

  private static class TableMetaTupleScheme extends org.apache.thrift.scheme.TupleScheme<TableMeta> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TableMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetTableIdentifier()) {
        optionals.set(0);
      }
      if (struct.isSetKeySpec()) {
        optionals.set(1);
      }
      if (struct.isSetProperties()) {
        optionals.set(2);
      }
      if (struct.isSetLocations()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetTableIdentifier()) {
        struct.tableIdentifier.write(oprot);
      }
      if (struct.isSetKeySpec()) {
        struct.keySpec.write(oprot);
      }
      if (struct.isSetProperties()) {
        {
          oprot.writeI32(struct.properties.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter100 : struct.properties.entrySet())
          {
            oprot.writeString(_iter100.getKey());
            oprot.writeString(_iter100.getValue());
          }
        }
      }
      if (struct.isSetLocations()) {
        {
          oprot.writeI32(struct.locations.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter101 : struct.locations.entrySet())
          {
            oprot.writeString(_iter101.getKey());
            oprot.writeString(_iter101.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TableMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.tableIdentifier = new com.netease.arctic.ams.api.TableIdentifier();
        struct.tableIdentifier.read(iprot);
        struct.setTableIdentifierIsSet(true);
      }
      if (incoming.get(1)) {
        struct.keySpec = new PrimaryKeySpec();
        struct.keySpec.read(iprot);
        struct.setKeySpecIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map102 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.properties = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map102.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key103;
          @org.apache.thrift.annotation.Nullable java.lang.String _val104;
          for (int _i105 = 0; _i105 < _map102.size; ++_i105)
          {
            _key103 = iprot.readString();
            _val104 = iprot.readString();
            struct.properties.put(_key103, _val104);
          }
        }
        struct.setPropertiesIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TMap _map106 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.locations = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map106.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key107;
          @org.apache.thrift.annotation.Nullable java.lang.String _val108;
          for (int _i109 = 0; _i109 < _map106.size; ++_i109)
          {
            _key107 = iprot.readString();
            _val108 = iprot.readString();
            struct.locations.put(_key107, _val108);
          }
        }
        struct.setLocationsIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

