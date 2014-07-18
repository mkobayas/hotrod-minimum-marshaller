/*
 * Copyright 2014 Masazumi Kobayashi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mk300.infinispan.hotrod.marshaller;

import java.io.IOException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;
import org.mk300.marshal.minimum.MinimumMarshaller;
import org.mk300.marshal.minimum.io.BAInputStream;
import org.mk300.marshal.minimum.io.OInputStream;

/**
 * 
 * @author mkobayas@redhat.com
 *
 */
public class HotrodMinimumMarshaller implements Marshaller {

	public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
		return MinimumMarshaller.marshal(obj, estimatedSize);
	}

	public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
		return MinimumMarshaller.marshal(obj);
	}

	public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
		return MinimumMarshaller.unmarshal(buf);
	}

	// Client-Serverモード, compatibleMode=falseでは呼ばれない。
	// 一応、compatibleMode=trueの場合は、PersistenceStoreやjgroupのレイヤーから呼ばれるので実装しているが、その場合、
	// JDGのクラスも設定ファイルに追加する必要があり、正常に動作させるのが至難の技である。
	public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
		BAInputStream bais = new BAInputStream(buf, offset, length);		
		OInputStream ois = new OInputStream(bais);
		return ois.readObject();
	}

	// Client-Serverモード, compatibleMode=falseでは呼ばれない。
	// 一応、compatibleMode=trueの場合は、PersistenceStoreやjgroupのレイヤーから呼ばれるので実装しているが、その場合、
	// JDGのクラスも設定ファイルに追加する必要があり、正常に動作させるのが至難の技である。
	public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
		byte[] bin = MinimumMarshaller.marshal(o);
		ByteBufferFactoryImpl factory = new ByteBufferFactoryImpl();
		return factory.newByteBuffer(bin, 0, bin.length);
	}

	// 呼ばれない。
	public boolean isMarshallable(Object o) throws Exception {
		return true;
	}

	public BufferSizePredictor getBufferSizePredictor(Object o) {
		return new DummyBufferSizePredictor();
	}
	
	public static class DummyBufferSizePredictor implements BufferSizePredictor {

		// 呼ばれない。
		public int nextSize(Object obj) {
			return 256;
		}
		
		// 呼ばれない。
		public void recordSize(int previousSize) {
			// no-op
		}		
	}
}
