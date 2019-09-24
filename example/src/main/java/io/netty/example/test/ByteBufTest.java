package io.netty.example.test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

/**
 * @author yangjing
 */
public class ByteBufTest {


	public static void main(String[] args) {

			int minNewCapacity =  10;
			int threshold = 5;
			int i = minNewCapacity / threshold * threshold;
			System.out.println(i);
	}
}
