package com.titan.raft.transport;

import com.titan.raft.protocol.RaftMessages;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.DirectByteBuffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RpcServer {
    private static final Logger logger = LoggerFactory.getLogger(RpcServer.class);

    private final int port;
    private final RpcDispatcher dispatcher;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    
    private Channel serverChannel;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private static final VarHandle RUNNING_HANDLE;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            RUNNING_HANDLE = l.findVarHandle(RpcServer.class, "isRunningInt", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    @SuppressWarnings("unused")
    private volatile int isRunningInt = 0;

    public RpcServer(int port, RpcDispatcher dispatcher) {
        this.port = port;
        this.dispatcher = dispatcher;
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
    }

    public CompletableFuture<Void> start() {
        if (!running.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(new IllegalStateException("RpcServer is already running"));
        }
        RUNNING_HANDLE.setVolatile(this, 1);

        CompletableFuture<Void> future = new CompletableFuture<>();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .option(ChannelOption.SO_BACKLOG, 1024)
         .option(ChannelOption.SO_REUSEADDR, true)
         .childOption(ChannelOption.TCP_NODELAY, true)
         .childOption(ChannelOption.SO_KEEPALIVE, true)
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ChannelPipeline p = ch.pipeline();
                 p.addLast(new RaftFrameDecoder());
                 p.addLast(new RaftFrameEncoder());
                 p.addLast(new RpcServerHandler(dispatcher));
             }
         });

        b.bind(port).addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                serverChannel = channelFuture.channel();
                logger.info("RpcServer successfully bound to port {}", port);
                future.complete(null);
            } else {
                running.set(false);
                RUNNING_HANDLE.setVolatile(this, 0);
                logger.error("RpcServer failed to bind to port {}", port, channelFuture.cause());
                future.completeExceptionally(channelFuture.cause());
            }
        });

        return future;
    }

    public CompletableFuture<Void> stop() {
        if (!running.compareAndSet(true, false)) {
            return CompletableFuture.completedFuture(null);
        }
        RUNNING_HANDLE.setVolatile(this, 0);

        CompletableFuture<Void> future = new CompletableFuture<>();
        if (serverChannel != null) {
            serverChannel.close().addListener(cf -> {
                Future<?> bossShutdown = bossGroup.shutdownGracefully();
                Future<?> workerShutdown = workerGroup.shutdownGracefully();
                
                bossShutdown.addListener(bf -> {
                    workerShutdown.addListener(wf -> {
                        logger.info("RpcServer stopped completely.");
                        future.complete(null);
                    });
                });
            });
        } else {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            future.complete(null);
        }
        return future;
    }

    public boolean isRunning() {
        return (int) RUNNING_HANDLE.getVolatile(this) == 1;
    }

    public static final class RaftFrameDecoder extends ByteToMessageDecoder {
        private static final int MAGIC_HEADER = 0x54495441; // "TITA"
        private static final int HEADER_LENGTH = 8; // 4 bytes magic, 4 bytes payload length

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < HEADER_LENGTH) {
                return;
            }

            in.markReaderIndex();
            int magic = in.readInt();
            if (magic != MAGIC_HEADER) {
                in.resetReaderIndex();
                ctx.close();
                throw new IllegalArgumentException("Invalid magic header received: " + Integer.toHexString(magic));
            }

            int payloadLength = in.readInt();
            if (payloadLength < 0 || payloadLength > 64 * 1024 * 1024) { // 64MB max frame limit
                in.resetReaderIndex();
                ctx.close();
                throw new IllegalArgumentException("Invalid payload length: " + payloadLength);
            }

            if (in.readableBytes() < payloadLength) {
                in.resetReaderIndex();
                return;
            }

            ByteBuf payload = in.readSlice(payloadLength);
            out.add(payload.retain());
        }
    }

    public static final class RaftFrameEncoder extends MessageToByteEncoder<ByteBuf> {
        private static final int MAGIC_HEADER = 0x54495441;

        @Override
        protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) {
            int readable = msg.readableBytes();
            out.ensureWritable(4 + 4 + readable);
            out.writeInt(MAGIC_HEADER);
            out.writeInt(readable);
            out.writeBytes(msg);
        }
    }

    public static final class RpcServerHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final RpcDispatcher dispatcher;

        public RpcServerHandler(RpcDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            int readable = msg.readableBytes();
            ByteBuffer nioBuffer = msg.internalNioBuffer(msg.readerIndex(), readable);
            
            ByteBuffer directSlice;
            if (nioBuffer.isDirect()) {
                directSlice = nioBuffer.slice();
            } else {
                directSlice = ByteBuffer.allocateDirect(readable);
                directSlice.put(nioBuffer);
                directSlice.flip();
            }

            try {
                ByteBuffer responseBuffer = dispatcher.dispatch(directSlice);
                if (responseBuffer != null) {
                    ByteBuf responseByteBuf = ctx.alloc().directBuffer(responseBuffer.remaining());
                    responseByteBuf.writeBytes(responseBuffer);
                    ctx.writeAndFlush(responseByteBuf);
                }
            } catch (Exception e) {
                logger.error("Exception handling RPC request in RpcServerHandler", e);
                ctx.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception in channel pipeline for {}", ctx.channel().remoteAddress(), cause);
            ctx.close();
        }
    }

    public interface RpcDispatcher {
        ByteBuffer dispatch(ByteBuffer requestPayload) throws IOException;
    }
}