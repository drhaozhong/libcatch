package org.apache.accumulo.examples.wikisearch.protobuf;


import com.google.protobuf.AbstractMessage;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;
import com.google.protobuf.UnknownFieldSet;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;

import static com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom;


public final class Uid {
	private Uid() {
	}

	public static void registerAllExtensions(ExtensionRegistry registry) {
	}

	public static final class List extends GeneratedMessage {
		private List() {
			initFields();
		}

		private List(boolean noInit) {
		}

		private static final Uid.List defaultInstance;

		public static Uid.List getDefaultInstance() {
			return Uid.List.defaultInstance;
		}

		public Uid.List getDefaultInstanceForType() {
			return Uid.List.defaultInstance;
		}

		public static final Descriptors.Descriptor getDescriptor() {
			return Uid.internal_static_protobuf_List_descriptor;
		}

		protected GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
			return Uid.internal_static_protobuf_List_fieldAccessorTable;
		}

		public static final int IGNORE_FIELD_NUMBER = 1;

		private boolean hasIGNORE;

		private boolean iGNORE_ = false;

		public boolean hasIGNORE() {
			return hasIGNORE;
		}

		public boolean getIGNORE() {
			return iGNORE_;
		}

		public static final int COUNT_FIELD_NUMBER = 2;

		private boolean hasCOUNT;

		private long cOUNT_ = 0L;

		public boolean hasCOUNT() {
			return hasCOUNT;
		}

		public long getCOUNT() {
			return cOUNT_;
		}

		public static final int UID_FIELD_NUMBER = 3;

		private java.util.List<String> uID_ = Collections.emptyList();

		public java.util.List<String> getUIDList() {
			return uID_;
		}

		public int getUIDCount() {
			return uID_.size();
		}

		public String getUID(int index) {
			return uID_.get(index);
		}

		private void initFields() {
		}

		public final boolean isInitialized() {
			if (!(hasIGNORE))
				return false;

			if (!(hasCOUNT))
				return false;

			return true;
		}

		public void writeTo(CodedOutputStream output) throws IOException {
			getSerializedSize();
			if (hasIGNORE()) {
				output.writeBool(1, getIGNORE());
			}
			if (hasCOUNT()) {
				output.writeUInt64(2, getCOUNT());
			}
			for (String element : getUIDList()) {
				output.writeString(3, element);
			}
			getUnknownFields().writeTo(output);
		}

		private int memoizedSerializedSize = -1;

		public int getSerializedSize() {
			int size = memoizedSerializedSize;
			if (size != (-1))
				return size;

			size = 0;
			if (hasIGNORE()) {
				size += CodedOutputStream.computeBoolSize(1, getIGNORE());
			}
			if (hasCOUNT()) {
				size += CodedOutputStream.computeUInt64Size(2, getCOUNT());
			}
			{
				int dataSize = 0;
				for (String element : getUIDList()) {
					dataSize += CodedOutputStream.computeStringSizeNoTag(element);
				}
				size += dataSize;
				size += 1 * (getUIDList().size());
			}
			size += getUnknownFields().getSerializedSize();
			memoizedSerializedSize = size;
			return size;
		}

		public static Uid.List parseFrom(ByteString data) throws InvalidProtocolBufferException {
			return Uid.List.newBuilder().mergeFrom(data).buildParsed();
		}

		public static Uid.List parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
			return Uid.List.newBuilder().mergeFrom(data, extensionRegistry).buildParsed();
		}

		public static Uid.List parseFrom(byte[] data) throws InvalidProtocolBufferException {
			return Uid.List.newBuilder().mergeFrom(data).buildParsed();
		}

		public static Uid.List parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
			return Uid.List.newBuilder().mergeFrom(data, extensionRegistry).buildParsed();
		}

		public static Uid.List parseFrom(InputStream input) throws IOException {
			return Uid.List.newBuilder().mergeFrom(input).buildParsed();
		}

		public static Uid.List parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
			return Uid.List.newBuilder().mergeFrom(input, extensionRegistry).buildParsed();
		}

		public static Uid.List parseDelimitedFrom(InputStream input) throws IOException {
			Uid.List.Builder builder = Uid.List.newBuilder();
			if (builder.mergeDelimitedFrom(input)) {
				return builder.buildParsed();
			}else {
				return null;
			}
		}

		public static Uid.List parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
			Uid.List.Builder builder = Uid.List.newBuilder();
			if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
				return builder.buildParsed();
			}else {
				return null;
			}
		}

		public static Uid.List parseFrom(CodedInputStream input) throws IOException {
			return Uid.List.newBuilder().mergeFrom(input).buildParsed();
		}

		public static Uid.List parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
			return Uid.List.newBuilder().mergeFrom(input, extensionRegistry).buildParsed();
		}

		public static Uid.List.Builder newBuilder() {
			return Uid.List.Builder.create();
		}

		public static Uid.List.Builder newBuilder(Uid.List prototype) {
			return Uid.List.newBuilder().mergeFrom(prototype);
		}

		public Uid.List.Builder toBuilder() {
			return Uid.List.newBuilder(this);
		}

		public static final class Builder extends GeneratedMessage.Builder<Uid.List.Builder> {
			private Uid.List result;

			private Builder() {
			}

			private static Uid.List.Builder create() {
				Uid.List.Builder builder = new Uid.List.Builder();
				builder.result = new Uid.List();
				return builder;
			}

			protected Uid.List internalGetResult() {
				return result;
			}

			public Uid.List.Builder clear() {
				if ((result) == null) {
					throw new IllegalStateException("Cannot call clear() after build().");
				}
				result = new Uid.List();
				return this;
			}

			public Uid.List.Builder clone() {
				return Uid.List.Builder.create().mergeFrom(result);
			}

			public Descriptors.Descriptor getDescriptorForType() {
				return Uid.List.getDescriptor();
			}

			public Uid.List getDefaultInstanceForType() {
				return Uid.List.getDefaultInstance();
			}

			public boolean isInitialized() {
				return result.isInitialized();
			}

			public Uid.List build() {
				if (((result) != null) && (!(isInitialized()))) {
					throw AbstractMessage.Builder.newUninitializedMessageException(result);
				}
				return buildPartial();
			}

			private Uid.List buildParsed() throws InvalidProtocolBufferException {
				if (!(isInitialized())) {
					throw AbstractMessage.Builder.newUninitializedMessageException(result).asInvalidProtocolBufferException();
				}
				return buildPartial();
			}

			public Uid.List buildPartial() {
				if ((result) == null) {
					throw new IllegalStateException("build() has already been called on this Builder.");
				}
				if ((result.uID_) != (Collections.EMPTY_LIST)) {
					result.uID_ = Collections.unmodifiableList(result.uID_);
				}
				Uid.List returnMe = result;
				result = null;
				return returnMe;
			}

			public Uid.List.Builder mergeFrom(Message other) {
				if (other instanceof Uid.List) {
					return mergeFrom(((Uid.List) (other)));
				}else {
					super.mergeFrom(other);
					return this;
				}
			}

			public Uid.List.Builder mergeFrom(Uid.List other) {
				if (other == (Uid.List.getDefaultInstance()))
					return this;

				if (other.hasIGNORE()) {
					setIGNORE(other.getIGNORE());
				}
				if (other.hasCOUNT()) {
					setCOUNT(other.getCOUNT());
				}
				if (!(other.uID_.isEmpty())) {
					if (result.uID_.isEmpty()) {
						result.uID_ = new ArrayList<String>();
					}
					result.uID_.addAll(other.uID_);
				}
				this.mergeUnknownFields(other.getUnknownFields());
				return this;
			}

			public Uid.List.Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
				UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder(this.getUnknownFields());
				while (true) {
					int tag = input.readTag();
					switch (tag) {
						case 0 :
							this.setUnknownFields(unknownFields.build());
							return this;
						default :
							{
								if (!(parseUnknownField(input, unknownFields, extensionRegistry, tag))) {
									this.setUnknownFields(unknownFields.build());
									return this;
								}
								break;
							}
						case 8 :
							{
								setIGNORE(input.readBool());
								break;
							}
						case 16 :
							{
								setCOUNT(input.readUInt64());
								break;
							}
						case 26 :
							{
								addUID(input.readString());
								break;
							}
					}
				} 
			}

			public boolean hasIGNORE() {
				return result.hasIGNORE();
			}

			public boolean getIGNORE() {
				return result.getIGNORE();
			}

			public Uid.List.Builder setIGNORE(boolean value) {
				result.hasIGNORE = true;
				result.iGNORE_ = value;
				return this;
			}

			public Uid.List.Builder clearIGNORE() {
				result.hasIGNORE = false;
				result.iGNORE_ = false;
				return this;
			}

			public boolean hasCOUNT() {
				return result.hasCOUNT();
			}

			public long getCOUNT() {
				return result.getCOUNT();
			}

			public Uid.List.Builder setCOUNT(long value) {
				result.hasCOUNT = true;
				result.cOUNT_ = value;
				return this;
			}

			public Uid.List.Builder clearCOUNT() {
				result.hasCOUNT = false;
				result.cOUNT_ = 0L;
				return this;
			}

			public java.util.List<String> getUIDList() {
				return Collections.unmodifiableList(result.uID_);
			}

			public int getUIDCount() {
				return result.getUIDCount();
			}

			public String getUID(int index) {
				return result.getUID(index);
			}

			public Uid.List.Builder setUID(int index, String value) {
				if (value == null) {
					throw new NullPointerException();
				}
				result.uID_.set(index, value);
				return this;
			}

			public Uid.List.Builder addUID(String value) {
				if (value == null) {
					throw new NullPointerException();
				}
				if (result.uID_.isEmpty()) {
					result.uID_ = new ArrayList<String>();
				}
				result.uID_.add(value);
				return this;
			}

			public Uid.List.Builder addAllUID(Iterable<? extends String> values) {
				if (result.uID_.isEmpty()) {
					result.uID_ = new ArrayList<String>();
				}
				super.addAll(values, result.uID_);
				return this;
			}

			public Uid.List.Builder clearUID() {
				result.uID_ = Collections.emptyList();
				return this;
			}

			public GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
				return null;
			}
		}

		static {
			defaultInstance = new Uid.List(true);
			Uid.internalForceInit();
			Uid.List.defaultInstance.initFields();
		}

		public Message.Builder newBuilderForType() {
			return null;
		}

		public Message.Builder newBuilderForType(GeneratedMessage.BuilderParent para0) {
			return null;
		}
	}

	private static Descriptors.Descriptor internal_static_protobuf_List_descriptor;

	private static GeneratedMessage.FieldAccessorTable internal_static_protobuf_List_fieldAccessorTable;

	public static Descriptors.FileDescriptor getDescriptor() {
		return Uid.descriptor;
	}

	private static Descriptors.FileDescriptor descriptor;

	static {
		String[] descriptorData = new String[]{ "\n\tUid.proto\u0012\bprotobuf\"2\n\u0004List\u0012\u000e\n\u0006IGNORE\u0018" + ("\u0001 \u0002(\b\u0012\r\n\u0005COUNT\u0018\u0002 \u0002(\u0004\u0012\u000b\n\u0003UID\u0018\u0003 \u0003(\tB\f\n\bpro" + "tobufH\u0001") };
		Descriptors.FileDescriptor.InternalDescriptorAssigner assigner = new Descriptors.FileDescriptor.InternalDescriptorAssigner() {
			public ExtensionRegistry assignDescriptors(Descriptors.FileDescriptor root) {
				Uid.descriptor = root;
				Uid.internal_static_protobuf_List_descriptor = Uid.getDescriptor().getMessageTypes().get(0);
				Uid.internal_static_protobuf_List_fieldAccessorTable = new GeneratedMessage.FieldAccessorTable(Uid.internal_static_protobuf_List_descriptor, new String[]{ "IGNORE", "COUNT", "UID" }, Uid.List.class, Uid.List.Builder.class);
				return null;
			}
		};
		internalBuildGeneratedFileFrom(descriptorData, new Descriptors.FileDescriptor[]{  }, assigner);
	}

	public static void internalForceInit() {
	}
}

