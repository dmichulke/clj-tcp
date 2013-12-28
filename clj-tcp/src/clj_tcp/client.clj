(ns clj-tcp.client
   (:require [clojure.tools.logging :refer [info error]]
             [clj-tcp.codec :refer [byte-decoder default-encoder buffer->bytes]]
             [clojure.core.async :refer [chan >!! go >! <! <!! thread timeout alts!!]])
   (:import  
            [clj_tcp.util PipelineUtil]
            [io.netty.handler.codec ByteToMessageDecoder]
            [java.net InetSocketAddress]
            [java.util List]
            [java.util.concurrent.atomic AtomicInteger AtomicBoolean]
            [io.netty.util CharsetUtil]
            [io.netty.buffer Unpooled ByteBuf ByteBufUtil]
            [io.netty.channel SimpleChannelInboundHandler ChannelPipeline ChannelFuture Channel ChannelHandler ChannelInboundHandlerAdapter ChannelInitializer ChannelInitializer ChannelHandlerContext ChannelFutureListener]
            [io.netty.channel.nio NioEventLoopGroup]
            [io.netty.util.concurrent GenericFutureListener Future EventExecutorGroup]
            [io.netty.bootstrap Bootstrap]
            [io.netty.channel.socket.nio NioSocketChannel]))

(defrecord Client [group channel-f write-ch read-ch internal-error-ch error-ch ^AtomicInteger reconnect-count ^AtomicBoolean closed])


(defrecord Reconnected [^Client client cause])
(defrecord Pause [time])
(defrecord Stop [])
(defrecord Poison [])
(defrecord FailedWrite [v])


(defn close-client [{:keys [group channel-f]}]
  (if channel-f
    (-> ^ChannelFuture channel-f ^Channel .channel .closeFuture)))

(defn close-all [{:keys [group closed] :as conf}]
  (close-client conf)
  (if group
    (-> group .shutdownGracefully .sync))
  (if (not closed)
    (.set ^AtomicBoolean closed true)))


(defn client-handler [{:keys [group read-ch internal-error-ch write-ch]}]
  (proxy [SimpleChannelInboundHandler]
    []
    (channelActive [^ChannelHandlerContext ctx]
      ;(.writeAndFlush ctx (Unpooled/copiedBuffer "Netty Rocks1" CharsetUtil/UTF_8))
      )
    (channelRead0 [^ChannelHandlerContext ctx in]
      (>!! read-ch (if (instance? ByteBuf in) (buffer->bytes in)  in))
      )
    (exceptionCaught [^ChannelHandlerContext ctx cause]
      (error "Client-handler exception caught " cause)
      (error cause (>!! [cause ctx]) )
      (.close ctx))))

    
(defn ^ChannelInitializer client-channel-initializer [{:keys [^EventExecutorGroup group read-ch internal-error-ch write-ch handlers] :as conf}]
  (let [group (NioEventLoopGroup.)]
	  (proxy [ChannelInitializer]
	    []
	    (initChannel [^Channel ch]
        (try 
	        ;add the last default read handler that will send all read objects to the read-ch blocking if full
         ;add any extra handlers e.g. for encoding or deconding
         (let [^ChannelPipeline pipeline (.pipeline ch)] 
	         (if handlers
	            (PipelineUtil/addLast pipeline group (map #(%) handlers)))
	         
	           (PipelineUtil/addLast pipeline group [(client-handler conf)]))
         
         (catch Exception e (do 
                              (error (str "channel initializer error " e) e)
                              (go (>! internal-error-ch [e nil]))
                              )))
	      ))))

(defn exception-listener [v {:keys [internal-error-ch]}]
  "Returns a GenericFutureListener instance
   that on completion checks the Future, if any exception
   an error is sent to the error-ch"
  (reify GenericFutureListener
    (operationComplete [this f]
       (if (not (.isSuccess ^Future f))
         (if-let [cause (.cause ^Future f)]
               (do (error "operation complete cause " cause)
                   (go (>! internal-error-ch [cause (->FailedWrite v)])))
           )))))

(defn close-listener [^Client client {:keys [internal-error-ch]}]
  "Close a client after a write operation has been completed"
  (reify GenericFutureListener
    (operationComplete [this f]
       (thread 
               (try
                  (close-client client)
                  (catch Exception e (do
                                       (error (str "Close listener error " e)  e)
                                       (>!! internal-error-ch [e nil])
                                       )))))))
           

(defn write! [{:keys [write-ch]} v]
  "Writes and blocks if the write-ch is full"
  (>!! write-ch v))

(defn read! 
  ([{:keys [read-ch]} timeout-ms]
    (first 
      (alts!!
		     [read-ch
		     (timeout timeout-ms)])))
  ([{:keys [read-ch]}]
  "Reads from the read-ch and blocks if no data is available"
  (<!! read-ch)))



(defn read-error 
   ([{:keys [error-ch]} timeout-ms]
    (first 
      (alts!!
		     [error-ch
		     (timeout timeout-ms)])))
    
  ([{:keys [error-ch]}]
  "Reads from the error-ch and blocks if no data is available"
  (<!! error-ch)))



(defn- do-write [^Client client ^bytes v close-after-write {:keys [internal-error-ch] :as conf}]
  "Writes to the channel, this operation is non blocking and a exception listener is added to the write's ChannelFuture
   to send any errors to the internal-error-ch"
  (try 
    (do 
      (info "Write and flush value " v)
     (let [ch-f (-> client ^ChannelFuture (:channel-f) ^Channel (.channel) ^ChannelFuture (.writeAndFlush v) (.addListener ^ChannelFutureListener (exception-listener v conf)))]
       (if close-after-write
         (.addListener ch-f ^ChannelFutureListener (close-listener client conf)))))
     (catch Exception e (do 
                          (error (str "Error in do-write " e) e)
                          (>!! internal-error-ch [e v])
                          ))))


(defn start-client [host port {:keys [group read-ch internal-error-ch error-ch write-ch handlers] :as conf 
                                 :or {group (NioEventLoopGroup.) 
                                      read-ch (chan 100) internal-error-ch (chan 100) error-ch (100) write-ch (chan 100)}}]
  "Start a Client instance with read-ch, write-ch and internal-error-ch"
  (try
  (let [g (if group group (NioEventLoopGroup.))
        b (Bootstrap.)]
    (-> b (.group g)
      ^Bootstrap (.channel NioSocketChannel)
      ^Bootstrap (.remoteAddress (InetSocketAddress. (str host) (int port)))
      ^Bootstrap (.handler ^ChannelInitializer (client-channel-initializer conf)))
    (let [ch-f (.connect b)]
      (.sync ch-f)
      (->Client g ch-f write-ch read-ch internal-error-ch error-ch (AtomicInteger.) (AtomicBoolean. false))))
  (catch Exception e (do
                       (.printStackTrace e)
                       (error (str "Error starting client " e) e)
                       (>!! internal-error-ch [e nil])
                       ))))
    
(defn read-print-ch [n ch]
  (go 
    (loop [ch1 ch]
      (let [c (<! ch1)]
         (if (instance? Reconnected c)
           (do 
             ;(info "Reconnected " (:cause c) " ch1 " ch1 " new ch " (-> c :client :read-ch))
             (recur (-> c :client :read-ch)))
           (do 
             (info n " = " c)
             (recur ch1)))))))

(defn read-print-in [{:keys [read-ch]}]
  (read-print-ch "read" read-ch))

(defn write-poison [{:keys [write-ch read-ch internal-error-ch]}]
  (go (>! write-ch [(->Poison) nil] ))
	(go (>! read-ch (->Poison) ))
  (go (>! internal-error-ch [(->Poison) nil] )))

(defn client [host port {:keys [handlers
                                  retry-limit
                                  write-buff read-buff error-buff
                                  write-timeout read-timeout] 
                           :or {handlers [default-encoder] retry-limit 5
                                write-buff 100 read-buff 100 error-buff 1000 reuse-client false write-timeout 1500 read-timeout 1500} }]
  (let [ write-ch (chan write-buff) 
         read-ch (chan read-buff)
         internal-error-ch (chan error-buff)
         error-ch (chan error-buff)
         g (NioEventLoopGroup.)
         conf {:group g :write-ch write-ch :read-ch read-ch :internal-error-ch internal-error-ch :error-ch error-ch :handlers handlers}
         client (start-client host port conf) ]
    
    (if (not client)
      (do 
        (let [cause (read-error {:internal-error-ch internal-error-ch} 200)]
	        (close-all client)
	        (throw (RuntimeException. "Unable to create client" (first cause))))))
    
    ;async read off internal-error-ch
    (go 
      (loop [local-client client]
        (let [[v o] (<! internal-error-ch)]
          (error "read error from internal-error-ch " v)
          (if (> (.get (:reconnect-count local-client)) retry-limit) ;check reconnect count
             (do ;if the same connection has been reconnected too many times close
               (write-poison local-client)
               (close-all local-client)
               (>! error-ch [v o]) ;send the error channel
               )
             (do
		          (if (instance? Poison v)
		            (if local-client (close-client local-client)) ;poinson pill end loop
		          (do
		            (if (instance? Exception v) (error v v)) 
		              
		            ;on error, pause writing, and close client
			          (>! write-ch (->Pause 1000))
		            (close-client local-client)
		          
		           (let [c 
		                 (loop [acc 0] ;reconnect in loop
						            (if (>= acc retry-limit)
						              (do
		                        ;if limit reached send poinson to all channels and call close all on client, end loop
						                (error "Retry limit reached, closing all channels and connections")
						                (write-poison local-client)
						                (close-all local-client)
                            (>! error-ch [v o]) ;write the exception to the error-ch
					                  nil
						              )
							            (let [v1 
		                             (try 
											              (let [c (start-client host port conf)
											                    reconnected (->Reconnected c v)]
						                              ;if connected, send Reconnected instance to all channels and return c, this c is assigned to the loop using recur
													                (.getAndIncrement ^AtomicInteger (:reconnect-count c))
													                (>! read-ch reconnected)
													                (>! write-ch reconnected)
											                    c)
											              (catch Exception e (do
											                                   (error (str "Error while doing retry " e) e)
		                                                     e ;return exception to v, due to a bug in core async http://dev.clojure.org/jira/browse/ASYNC-48, we cannot recur here
						                                             )))]
		                            (if (instance? Exception v1) ;if v is an exception recur
		                              (recur (inc acc))
		                              v1) ;else return the value (this is c, the connection)
		                            )))]
		                      
		                      (if (and (instance? FailedWrite o) c)
		                        (do 
		                            (info "retry failed write: ")
		                            (<! (timeout 500))
		                            (>! write-ch (:v o))))
		                      
		                      (recur c)))))
		           
			          ))))
		                
                
              
     ;async read off write-ch     
     (go  
	      (loop [local-client client]
           (let [v (<! write-ch)]
		          (if (instance? Stop v) nil ;if stop exit loop
		             (do 
                   (try 
                      (cond (instance? Reconnected v) (do (recur (:client v))) ;if reconnect recur with the new client
					             (instance? Pause v) (do (<! (timeout (:time v))) (recur local-client)) ;if pause, wait :time millis, then recur loop
					             :else
				                (do ;else write the value to the client channel
                           
						               (do-write local-client v false conf)))
                           (if (> (.get ^AtomicInteger (:reconnect-count local-client)) 0) (.set ^AtomicInteger (:reconnect-count local-client) 0))
                      
							        (catch Exception e (do ;send any exception to the internal-error-ch
								                            (error "!!!!! Error while writing " e)  
								                            (go (>! internal-error-ch [e nil]))
                                    )))
                      (if (not (instance? Stop v))
                         (recur local-client)) ;if not stop recur loop
                      )))))
    
    
		    client))         


       
		    