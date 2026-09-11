(ns engi-node.worker
  "A validator's inbound side: a Worker that accepts peer WebSockets.

  `engi.net.ws` dials out and `engi.net.server` accepts. This is the accepting
  half deployed somewhere that CANNOT dial out — a Cloudflare Worker has no
  outbound WebSocket client in a Durable Object's lifetime the way a Node
  process does, so a mesh containing one only works if the Worker is the thing
  being called.

  It is not a validator. It runs no pacemaker and casts no votes: it accepts
  connections, decodes messages with the same `engi.wire` a validator uses,
  and reports what it saw. That is enough to answer the question this exists
  to answer — does the transport work in a Worker — without pretending to a
  consensus role nothing here is ready for.

  `/head` says so, because a service that stays quiet about what it is will be
  assumed to be the other thing."
  (:require [engi.net :as net]
            [engi.net.server :as srv]
            [engi.attest :as att]
            [engi.attest.ed25519 :as ed]
            [engi.wire :as wire]))

(def ^:const code-version "2")
(def ^:const chain-id "engi-devnet-1")
(def ^:const quorum 3)

(defn- json [x status]
  (js/Response. (js/JSON.stringify (clj->js x))
                #js {:status status
                     :headers #js {"content-type" "application/json"
                                   "access-control-allow-origin" "*"}}))

(deftype Hub [^js do-state ^js env]
  Object
  (init [this]
    (when-not (.-registry this)
      (set! (.-registry this) (atom {}))
      (set! (.-seen this) (atom []))
      (set! (.-seq this) (atom 0))))

  (accept [this ^js request]
    (.init this)
    (let [pair (js/WebSocketPair.)
          client (aget pair "0")
          server (aget pair "1")
          peer (str "peer-" (swap! (.-seq this) inc))]
      (.accept server)
      (srv/attach! (.-registry this) peer server
                   {:on-message (fn [p m]
                                  ;; A new-view carries a certificate. Checking
                                  ;; it here is the point: until now the Worker
                                  ;; only proved a message could arrive, not
                                  ;; that what arrived could be believed.
                                  (if-let [qc (:high-qc m)]
                                    (-> (ed/verify-certificate! qc chain-id quorum)
                                        (.then (fn [r]
                                                 (swap! (.-seen this) conj
                                                        {:peer p :type (name (:type m))
                                                         :signed (att/signed? qc)
                                                         :witnesses (count (:engi.qc/witnesses qc))
                                                         :verified (nil? r)
                                                         :reason (when r (name r))}))))
                                    (swap! (.-seen this) conj
                                           {:peer p :type (name (:type m))
                                            :signed false}))
                                  ;; answer with something the peer can decode,
                                  ;; so the round trip is verifiable from the
                                  ;; other end rather than only from the log
                                  (try (.send server
                                              (js/JSON.stringify
                                               (clj->js (wire/encode
                                                         {:type :vote :witness :hub
                                                          :block-hash "H1" :height 1
                                                          :view 1}))))
                                       (catch :default _ nil)))
                    :on-reject (fn [p r]
                                 (swap! (.-seen this) conj
                                        {:peer p :rejected (name r)}))
                    :on-close (fn [_] nil)})
      (js/Response. nil #js {:status 101 :webSocket client})))

  (fetch [this ^js request]
    (.init this)
    (let [url (js/URL. (.-url request))
          path (.-pathname url)]
      (cond
        (= path "/peer")
        (if (= "websocket" (.get (.-headers request) "Upgrade"))
          (.accept this request)
          (json {:ok false :reason "expected-websocket-upgrade"} 426))

        (= path "/head")
        (json {:code-version code-version
               :peers (srv/live (.-registry this))
               :peer-count (srv/peer-count (.-registry this))
               :seen (vec (take-last 10 @(.-seen this)))
               :chain-id chain-id
               :role "accepts and VERIFIES certificates; runs no pacemaker and casts no votes"}
              200)

        :else (json {:ok false :reason "not-found"} 404)))))

(def handler
  #js {:fetch (fn [^js request ^js env _ctx]
                (let [^js ns* (.-HUB env)]
                  (.fetch (.get ns* (.idFromName ns* "main")) request)))})
