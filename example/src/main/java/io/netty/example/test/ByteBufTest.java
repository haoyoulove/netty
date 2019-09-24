package io.netty.example.test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

/**
 * @author yangjing
 */
public class ByteBufTest {


	public static void main(String[] args) {

		ByteBuf buf = ReferenceCountUtil.releaseLater(Unpooled.directBuffer(512));
	}
}
