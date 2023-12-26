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
import java.util.List;

import static com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom;


public final class TermWeight {
	private TermWeight() {
	}

	public static void registerAllExtensions(ExtensionRegistry registry) {
	}

	public static final class Info extends GeneratedMessage {
		private Info() {
			initFields();
		}

		private Info(boolean noInit) {
		}

		private static final TermWeight.Info defaultInstance;

		public static TermWeight.Info getDefaultInstance() {
			return TermWeight.Info.defaultInstance;
		}

		public TermWeight.Info getDefaultInstanceForType() {
			return TermWeight.Info.defaultInstance;
		}

		public static final Descriptors.Descriptor getDescriptor() {
			return TermWeight.internal_static_protobuf_Info_descriptor;
		}

		protected GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
			return TermWeight.internal_static_protobuf_Info_fieldAccessorTable;
		}

		public static final int NORMALIZEDTERMFREQUENCY_FIELD_NUMBER = 1;

		private boolean hasNormalizedTermFrequency;

		private float normalizedTermFrequency_ = 0.0F;

		public boolean hasNormalizedTermFrequency() {
			return hasNormalizedTermFrequency;
		}

		public float getNormalizedTermFrequency() {
			return normalizedTermFrequency_;
		}

		public static final int WORDOFFSET_FIELD_NUMBER = 2;

		private List<Integer> wordOffset_ = Collections.emptyList();

		public List<Integer> getWordOffsetList() {
			return wordOffset_;
		}

		public int getWordOffsetCount() {
			return wordOffset_.size();
		}

		public int getWordOffset(int index) {
			return wordOffset_.get(index);
		}

		private void initFields() {
		}

		public final boolean isInitialized() {
			if (!(hasNormalizedTermFrequency))
				return false;

			return true;
		}

		public void writeTo(CodedOutputStream output) throws IOException {
			getSerializedSize();
			if (hasNormalizedTermFrequency()) {
				output.writeFloat(1, getNormalizedTermFrequency());
			}
			for (int element : getWordOffsetList()) {
				output.writeUInt32(2, element);
			}
			getUnknownFields().writeTo(output);
		}

		private int memoizedSerializedSize = -1;

		public int getSerializedSize() {
			int size = memoizedSerializedSize;
			if (size != (-1))
				return size;

			size = 0;
			if (hasNormalizedTermFrequency()) {
				size += CodedOutputStream.computeFloatSize(1, getNormalizedTermFrequency());
			}
			{
				int dataSize = 0;
				for (int element : getWordOffsetList()) {
					dataSize += CodedOutputStream.computeUInt32SizeNoTag(element);
				}
				size += dataSize;
				size += 1 * (getWordOffsetList().size());
			}
			size += getUnknownFields().getSerializedSize();
			memoizedSerializedSize = size;
			return size;
		}

		public static TermWeight.Info parseFrom(ByteString data) throws InvalidProtocolBufferException {
			return TermWeight.Info.newBuilder().mergeFrom(data).buildParsed();
		}

		public static TermWeight.Info parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
			return TermWeight.Info.newBuilder().mergeFrom(data, extensionRegistry).buildParsed();
		}

		public static TermWeight.Info parseFrom(byte[] data) throws InvalidProtocolBufferException {
			return TermWeight.Info.newBuilder().mergeFrom(data).buildParsed();
		}

		public static TermWeight.Info parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
			return TermWeight.Info.newBuilder().mergeFrom(data, extensionRegistry).buildParsed();
		}

		public static TermWeight.Info parseFrom(InputStream input) throws IOException {
			return TermWeight.Info.newBuilder().mergeFrom(input).buildParsed();
		}

		public static TermWeight.Info parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
			return TermWeight.Info.newBuilder().mergeFrom(input, extensionRegistry).buildParsed();
		}

		public static TermWeight.Info parseDelimitedFrom(InputStream input) throws IOException {
			TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
			if (builder.mergeDelimitedFrom(input)) {
				return builder.buildParsed();
			}else {
				return null;
			}
		}

		public static TermWeight.Info parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
			TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
			if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
				return builder.buildParsed();
			}else {
				return null;
			}
		}

		public static TermWeight.Info parseFrom(CodedInputStream input) throws IOException {
			return TermWeight.Info.newBuilder().mergeFrom(input).buildParsed();
		}

		public static TermWeight.Info parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
			return TermWeight.Info.newBuilder().mergeFrom(input, extensionRegistry).buildParsed();
		}

		public static TermWeight.Info.Builder newBuilder() {
			return TermWeight.Info.Builder.create();
		}

		public TermWeight.Info.Builder newBuilderForType() {
			return TermWeight.Info.newBuilder();
		}

		public static TermWeight.Info.Builder newBuilder(TermWeight.Info prototype) {
			return TermWeight.Info.newBuilder().mergeFrom(prototype);
		}

		public TermWeight.Info.Builder toBuilder() {
			return TermWeight.Info.newBuilder(this);
		}

		public static final class Builder extends GeneratedMessage.Builder<TermWeight.Info.Builder> {
			private TermWeight.Info result;

			private Builder() {
			}

			private static TermWeight.Info.Builder create() {
				TermWeight.Info.Builder builder = new TermWeight.Info.Builder();
				builder.result = new TermWeight.Info();
				return builder;
			}

			protected TermWeight.Info internalGetResult() {
				return result;
			}

			public TermWeight.Info.Builder clear() {
				if ((result) == null) {
					throw new IllegalStateException("Cannot call clear() after build().");
				}
				result = new TermWeight.Info();
				return this;
			}

			public TermWeight.Info.Builder clone() {
				return TermWeight.Info.Builder.create().mergeFrom(result);
			}

			public Descriptors.Descriptor getDescriptorForType() {
				return TermWeight.Info.getDescriptor();
			}

			public TermWeight.Info getDefaultInstanceForType() {
				return TermWeight.Info.getDefaultInstance();
			}

			public boolean isInitialized() {
				return result.isInitialized();
			}

			public TermWeight.Info build() {
				if (((result) != null) && (!(isInitialized()))) {
					throw AbstractMessage.Builder.newUninitializedMessageException(result);
				}
				return buildPartial();
			}

			private TermWeight.Info buildParsed() throws InvalidProtocolBufferException {
				if (!(isInitialized())) {
					throw AbstractMessage.Builder.newUninitializedMessageException(result).asInvalidProtocolBufferException();
				}
				return buildPartial();
			}

			public TermWeight.Info buildPartial() {
				if ((result) == null) {
					throw new IllegalStateException("build() has already been called on this Builder.");
				}
				if ((result.wordOffset_) != (Collections.EMPTY_LIST)) {
					result.wordOffset_ = Collections.unmodifiableList(result.wordOffset_);
				}
				TermWeight.Info returnMe = result;
				result = null;
				return returnMe;
			}

			public TermWeight.Info.Builder mergeFrom(Message other) {
				if (other instanceof TermWeight.Info) {
					return mergeFrom(((TermWeight.Info) (other)));
				}else {
					super.mergeFrom(other);
					return this;
				}
			}

			public TermWeight.Info.Builder mergeFrom(TermWeight.Info other) {
				if (other == (TermWeight.Info.getDefaultInstance()))
					return this;

				if (other.hasNormalizedTermFrequency()) {
					setNormalizedTermFrequency(other.getNormalizedTermFrequency());
				}
				if (!(other.wordOffset_.isEmpty())) {
					if (result.wordOffset_.isEmpty()) {
						result.wordOffset_ = new ArrayList<Integer>();
					}
					result.wordOffset_.addAll(other.wordOffset_);
				}
				this.mergeUnknownFields(other.getUnknownFields());
				return this;
			}

			public TermWeight.Info.Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
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
						case 13 :
							{
								setNormalizedTermFrequency(input.readFloat());
								break;
							}
						case 16 :
							{
								addWordOffset(input.readUInt32());
								break;
							}
						case 18 :
							{
								int length = input.readRawVarint32();
								int limit = input.pushLimit(length);
								while ((input.getBytesUntilLimit()) > 0) {
									addWordOffset(input.readUInt32());
								} 
								input.popLimit(limit);
								break;
							}
					}
				} 
			}

			public boolean hasNormalizedTermFrequency() {
				return result.hasNormalizedTermFrequency();
			}

			public float getNormalizedTermFrequency() {
				return result.getNormalizedTermFrequency();
			}

			public TermWeight.Info.Builder setNormalizedTermFrequency(float value) {
				result.hasNormalizedTermFrequency = true;
				result.normalizedTermFrequency_ = value;
				return this;
			}

			public TermWeight.Info.Builder clearNormalizedTermFrequency() {
				result.hasNormalizedTermFrequency = false;
				result.normalizedTermFrequency_ = 0.0F;
				return this;
			}

			public List<Integer> getWordOffsetList() {
				return Collections.unmodifiableList(result.wordOffset_);
			}

			public int getWordOffsetCount() {
				return result.getWordOffsetCount();
			}

			public int getWordOffset(int index) {
				return result.getWordOffset(index);
			}

			public TermWeight.Info.Builder setWordOffset(int index, int value) {
				result.wordOffset_.set(index, value);
				return this;
			}

			public TermWeight.Info.Builder addWordOffset(int value) {
				if (result.wordOffset_.isEmpty()) {
					result.wordOffset_ = new ArrayList<Integer>();
				}
				result.wordOffset_.add(value);
				return this;
			}

			public TermWeight.Info.Builder addAllWordOffset(Iterable<? extends Integer> values) {
				if (result.wordOffset_.isEmpty()) {
					result.wordOffset_ = new ArrayList<Integer>();
				}
				super.addAll(values, result.wordOffset_);
				return this;
			}

			public TermWeight.Info.Builder clearWordOffset() {
				result.wordOffset_ = Collections.emptyList();
				return this;
			}

			public GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
				return null;
			}
		}

		static {
			defaultInstance = new TermWeight.Info(true);
			TermWeight.internalForceInit();
			TermWeight.Info.defaultInstance.initFields();
		}

		public Message.Builder newBuilderForType(GeneratedMessage.BuilderParent para0) {
			return null;
		}
	}

	private static Descriptors.Descriptor internal_static_protobuf_Info_descriptor;

	private static GeneratedMessage.FieldAccessorTable internal_static_protobuf_Info_fieldAccessorTable;

	public static Descriptors.FileDescriptor getDescriptor() {
		return TermWeight.descriptor;
	}

	private static Descriptors.FileDescriptor descriptor;

	static {
		String[] descriptorData = new String[]{ "\n\u0010TermWeight.proto\u0012\bprotobuf\";\n\u0004Info\u0012\u001f\n\u0017" + ("normalizedTermFrequency\u0018\u0001 \u0002(\u0002\u0012\u0012\n\nwordOff" + "set\u0018\u0002 \u0003(\rB\f\n\bprotobufH\u0001") };
		Descriptors.FileDescriptor.InternalDescriptorAssigner assigner = new Descriptors.FileDescriptor.InternalDescriptorAssigner() {
			public ExtensionRegistry assignDescriptors(Descriptors.FileDescriptor root) {
				TermWeight.descriptor = root;
				TermWeight.internal_static_protobuf_Info_descriptor = TermWeight.getDescriptor().getMessageTypes().get(0);
				TermWeight.internal_static_protobuf_Info_fieldAccessorTable = new GeneratedMessage.FieldAccessorTable(TermWeight.internal_static_protobuf_Info_descriptor, new String[]{ "NormalizedTermFrequency", "WordOffset" }, TermWeight.Info.class, TermWeight.Info.Builder.class);
				return null;
			}
		};
		internalBuildGeneratedFileFrom(descriptorData, new Descriptors.FileDescriptor[]{  }, assigner);
	}

	public static void internalForceInit() {
	}
}

