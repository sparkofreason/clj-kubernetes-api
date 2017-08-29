(ns kubernetes.api.util
  (:require [clojure.string :as str]
            [clojure.core.async :refer [go <! >! chan]]
            [org.httpkit.client :as http]
            [common-crypto.keyczar.core :as keyczar]
            [common-core.misc :as misc]
            [clojure.data.json :as json]))

(defn make-context [server]
  {:server server})

(defn- parameterize-path [path params]
  (reduce-kv (fn [s k v]
               (str/replace s (re-pattern (str "\\{" (name k) "\\}")) v))
             path
             params))

(defn dashed->camel [s]
  (str/replace s #"-([a-z])" #(str/upper-case (second %))))

(defn- query-str [query]
  (->> query
       (map (fn [[k v]] (str (dashed->camel (name k)) "=" v)))
       (str/join "&")))

(defn- url [{:keys [server]} path params query]
  (str server
       (parameterize-path path params)
       (if (empty? query) "" "?")
       (query-str query)))

(defn- token [username password]
  (when (and username password)
    (->> (str username ":" password)
         keyczar/string->bytes
         keyczar/bytes->base64
         (str "Basic "))))

(defn- content-type [method]
  (if (= method :patch)
    "application/merge-patch+json"
    "application/json"))

(defn parse-response [{:keys [status headers body error]}]
  (cond
    error {:success false :error error}
    :else (json/read-str body :key-fn keyword)))

(defn request [{:keys [username password] :as ctx} {:keys [method path params query body]}]
  (let [c (chan)
        ct (content-type method)
        authentication (token username password)]
    (http/request
     (cond-> {:url (url ctx path params query)
              :method method
              :as :text}
       body (assoc :body (json/write-str body)
                   :headers  (misc/assoc-if {"Content-Type" ct} "Authorization" authentication)))
     #(go (let [resp (parse-response %)]
            #_(println "Request" method path query body resp)
            (>! c resp))))
    c))
