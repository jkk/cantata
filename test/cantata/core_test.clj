(ns cantata.core-test
  (:require [clojure.test :refer [deftest testing is are run-tests use-fixtures]]
            [cantata.core :as c]
            [cantata.data-source :as cds]
            [cantata.io :as cio]
            [cantata.reflect :as cr]
            [clojure.java.io :as io]
            [clojure.string :as string])
  (:import java.util.zip.GZIPInputStream))

(def h2-spec
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem:film_store;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"})

(def mysql-spec
  {:subprotocol "mysql"
   :subname "//127.0.0.1/film_store"
   :user "film_store"
   :password "film_store"})

(def psql-spec
  {:subprotocol "postgresql"
   :subname "//localhost/film_store"
   :user "film_store"
   :password "film_store"})

(defn setup-db!
  "Sets up our database with a schema (no data yet)"
  [ds]
  (let [filename (str "create_" (cds/get-subprotocol ds) ".sql")
        sql (slurp (io/resource filename))]
    (doseq [stmt (remove string/blank? (string/split sql #";"))]
      (c/execute! ds stmt))))

(defn teardown-db!
  "Drops database schema"
  [ds]
  (let [filename (str "drop_" (cds/get-subprotocol ds) ".sql")
        sql (slurp (io/resource filename))]
    (doseq [stmt (remove string/blank? (string/split sql #";"))]
      (c/execute! ds stmt))))

(defn import-data! [ds]
  (cio/import-data! ds (-> (io/resource "data.edn.gz")
                         (io/input-stream)
                         (GZIPInputStream.)
                         (io/reader))))

;; Supplements reflected model
(def model
  {:film {:shortcuts {:actor :film-actor.actor
                      :category :film-category.category
                      :rental :inventory.rental
                      :renter :rental.customer
                      :store :inventory.store}}
   :actor {:shortcuts {:film :film-actor.film}}
   :category {:shortcuts {:film :film-category.film}}
   :customer {:shortcuts {:rented-film :rental.inventory.film
                          :city :address.city
                          :country :city.country
                          :city-name :city.city
                          :country-name :country.country}}
   :store {:shortcuts {:rental :inventory.rental
                       :payment :rental.payment
                       :manager :store-manager.manager
                       :city :address.city
                       :country :city.country
                       :city-name :city.city
                       :country-name :country.country}}})

#_(defonce ds
  (delay
    (c/data-source
      h2-spec model
      :init-fn setup-db!
      :reflect true
      ;:pooled true
      :clob-str true
      :blob-bytes true
      :joda-dates true
      :unordered-maps true)))

(def ds nil) ;will be redef'd for test runs

(use-fixtures
  :once
  (fn [f]
    (doseq [db-spec [h2-spec mysql-spec psql-spec]]
      (let [subprot (cds/get-subprotocol db-spec)]
        (println subprot "- Setting up DB")
        (setup-db! h2-spec)
        (println subprot "- Reflecting data model")
        (with-redefs [ds (c/data-source
                           h2-spec model
                           :reflect true
                           :unordered-maps true)]
          (println subprot "- Importing data")
          (import-data! ds)
          (println subprot "- Running tests")
          (f))
        (println subprot "- Tearing down DB")
        (teardown-db! h2-spec)))))

(defmacro are= [& body]
  `(are [expected# actual#] (= expected# actual#)
        ~@body))

(defn setify
  "Turns all nested sequences into sets"
  [x]
  (cond
    (map? x) (into {} (for [[k v] x] [k (setify v)]))
    (sequential? x) (set (map setify x))
    :else x))

(deftest test-querying
  (let [full-film {:from :film
                   :include [:category :actor :language :original-language]}
        canada-q {:from :film
                  :select [:title :language.name :actor.name]
                  :where [:= "Canada" :renter.country-name]}
        cn1 (set (c/queryf ds [:select :category.name
                               :where [:= 2 :film.renter.id]]))
        cn2 (set (c/getf :rented-film.category.name
                    (c/by-id ds :customer 2 [:select :rented-film.category.name])))
        kid-film-q {:from :film
                    :where [:and
                            [:in :rating ["G" "PG"]]
                            [:< 90 :length 100]]
                    :limit 5}
        rev-lang-q [:from :language :select [:name :_language.film.id]]
        cat-counts-q [:from :category
                      :select [:name :%count.id]
                      :where [:= 1 :film.renter.id]
                      :group-by :id]
        sales-by-store-q {:from :store
                          :select [:id :city-name :country-name
                                   :manager.first-name :manager.last-name
                                   :%sum.payment.amount]
                          :group-by [:id :city-name :country-name
                                     :manager.id :manager.first-name :manager.last-name]
                          :order-by [:country-name :city-name]}
        sales-by-cat-q [:from :category
                        :select [:name :%sum.film.rental.payment.amount]
                        :group-by :id]]
    (are=
      (:title (c/query1 ds [:from :film :where [:= 123 :id]])) "CASABLANCA SUPER" 
      (set (c/queryf ds [:select :film.actor :where [:= 123 :id]])) (set [{:name "KIRSTEN AKROYD", :id 92} {:name "WALTER TORN", :id 102} {:name "ANGELA WITHERSPOON", :id 144} {:name "REESE WEST", :id 197}])
      (c/query1 ds "select * from actor where id = 1") {:name "PENELOPE GUINESS", :id 1}
      (c/query1 ds ["select id from category where name=?" "Action"]) {:id 1}
      (count (c/query ds kid-film-q)) 5
      (setify (c/query ds [:from :film :select [:id :title] :include :actor :where [:= 1 :id]] :flat true)) (setify '({:actor.name "PENELOPE GUINESS", :actor.id 1, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "CHRISTIAN GABLE", :actor.id 10, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "LUCILLE TRACY", :actor.id 20, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "SANDRA PECK", :actor.id 30, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "JOHNNY CAGE", :actor.id 40, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "MENA TEMPLE", :actor.id 53, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "WARREN NOLTE", :actor.id 108, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "OPRAH KILMER", :actor.id 162, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "ROCK DUKAKIS", :actor.id 188, :title "ACADEMY DINOSAUR", :id 1} {:actor.name "MARY KEITEL", :actor.id 198, :title "ACADEMY DINOSAUR", :id 1}))
      (count (c/getf :_language.film (c/query ds rev-lang-q))) 500
      cn1 cn2
      cn2 (set '("Sports" "Classics" "Family" "Foreign" "Documentary" "Games" "Action" "New" "Animation"))
      (set (c/query ds cat-counts-q)) (set [{:%count.id 2, :name "Animation"} {:%count.id 3, :name "Classics"} {:%count.id 4, :name "Comedy"} {:%count.id 1, :name "Documentary"} {:%count.id 1, :name "Drama"} {:%count.id 1, :name "Family"} {:%count.id 1, :name "Games"} {:%count.id 1, :name "Music"} {:%count.id 2, :name "New"} {:%count.id 1, :name "Sci-Fi"} {:%count.id 1, :name "Travel"}])
      (setify (c/query ds sales-by-store-q)) (setify [{:manager [{:last-name "Stephens", :first-name "Jon"}], :%sum.payment.amount 8192.91M, :country-name "Australia", :city-name "Woodridge", :id 2} {:manager [{:last-name "Hillyer", :first-name "Mike"}], :%sum.payment.amount 8461.99M, :country-name "Canada", :city-name "Lethbridge", :id 1}])
      (setify (c/query ds sales-by-cat-q)) (setify [{:%sum.film.rental.payment.amount 1110.19M, :name "Action"} {:%sum.film.rental.payment.amount 1189.87M, :name "Animation"} {:%sum.film.rental.payment.amount 912.71M, :name "Children"} {:%sum.film.rental.payment.amount 809.86M, :name "Classics"} {:%sum.film.rental.payment.amount 1051.72M, :name "Comedy"} {:%sum.film.rental.payment.amount 806.84M, :name "Documentary"} {:%sum.film.rental.payment.amount 853.83M, :name "Drama"} {:%sum.film.rental.payment.amount 1133.96M, :name "Family"} {:%sum.film.rental.payment.amount 1134.32M, :name "Foreign"} {:%sum.film.rental.payment.amount 979.67M, :name "Games"} {:%sum.film.rental.payment.amount 842.07M, :name "Horror"} {:%sum.film.rental.payment.amount 1065.58M, :name "Music"} {:%sum.film.rental.payment.amount 1234.46M, :name "New"} {:%sum.film.rental.payment.amount 1510.41M, :name "Sci-Fi"} {:%sum.film.rental.payment.amount 1038.68M, :name "Sports"} {:%sum.film.rental.payment.amount 980.73M, :name "Travel"}]))
    (testing
      "query and querym parity"
      (let [r1 (setify (c/query ds [full-film :where [:= :id 123]]))
            r2 (setify (c/querym ds [full-film :where [:= :id 123]]))]
        (are= r1 r2))
      (let [r1 (setify (c/query ds canada-q))
            r2 (setify (c/querym ds canada-q))
            pq (c/prepare-query
                 ds (assoc canada-q :where [:= :?country :renter.country-name]))
            r3 (setify (c/query ds pq :params {:country "Canada"}))
            r4 (setify (c/by-id ds :film 1 [:include [:category :language]]))
            r5 (setify (c/query1 ds [:from :film :where [:= 1 :id] :include [:category :language]]))
            r6 (setify (c/querym1 ds [:from :film :where [:= 1 :id] :include [:category :language]]))]
        (are=
          47 (count r1)
          r1 r2
          r2 r3
          r4 r5
          r5 r6)))
    (testing
      "query-count"
      (are= 226 (c/query-count ds [:from :film :without :rental])))))

;; TODO: tests that exercise nest-in logic

(deftest test-custom-names
  (let [dm {:f {:db-name "film"
                :fields [{:name :foo :db-name "id"}
                         {:name :bar :db-name "title"}]
                :shortcuts {:a :fa.a}}
            :a {:db-name "actor"
                :fields [{:name :baz :db-name "id"}
                         {:name :qux :db-name "name"}]}
            :fa {:db-name "film_actor"
                 :fields [{:name :corge :db-name "film_id"}
                          {:name :grault :db-name "actor_id"}]
                 :pk [:corge :grault]
                 :rels [{:name :f :ename :f :key :corge :other-key :foo}
                        {:name :a :ename :a :key :grault :other-key :baz}]}}
        ds (c/data-source ds dm)]
    (are=
      (c/to-sql ds [:select :f.a.qux :where [:= 123 :foo]]) ["SELECT \"a\".\"name\" AS \"a.qux\" FROM \"film\" AS \"f\" LEFT JOIN \"film_actor\" AS \"fa\" ON \"f\".\"id\" = \"fa\".\"film_id\" LEFT JOIN \"actor\" AS \"a\" ON \"fa\".\"actor_id\" = \"a\".\"id\" WHERE 123 = \"f\".\"id\""])))

(deftest test-insert-update
  (let [ret-keys (c/insert! ds :language [{:name "Esperanto"}
                                          {:name "Klingon"}])
        updated-rows (c/update! ds :language {:name "tlhIngan Hol"}
                                [:= "Klingon" :name])]
    (is (every? number? ret-keys))
    (are= 2 (count ret-keys)
          1 updated-rows)))

(deftest test-save
  (let [fid (c/save! ds :film {:title "Lawrence of Arabia"
                               :language-id 1
                               :category [{:id 7}   ;Drama
                                          {:id 4}   ;Classics
                                          {:name "Epic"}]})
        epic (c/query1 ds [:from :category :where [:= :name "Epic"]])
        loa (c/by-id ds :film fid [:include [:category :language]])]
    (c/save! ds :film {:id 1001 :release-year 1962})
    (is (= 1962 (c/queryf1 ds [:select :film.release-year :where [:= fid :id]])))
    (is (pos? fid))
    (is (boolean epic))
    (is (= (:title loa) "Lawrence of Arabia"))
    (is (= (c/getf1 loa :language.name) "English"))
    (is (= (set (:category loa)) (set [epic {:id 7 :name "Drama"} {:id 4 :name "Classics"}])))
    (c/cascading-delete-ids! ds :film fid)
    ;; category should still be there - only junction rows removed
    (is (empty? (c/query ds [:from :film-category :where [:= fid :film-id]])))
    (is (= epic (c/query1 ds [:from :category :where [:= :name "Epic"]])))
    (c/cascading-delete-ids! ds :category (:id epic)))
  (let [[cid fid] (c/save! ds :film-category
                           {:film {:title "TITLE" :language-id 1}
                            :category {:name "CATEGORY"}})]
    (is (= "TITLE" (:title (c/by-id ds :film fid))))
    (is (= "CATEGORY" (:name (c/by-id ds :category cid))))
    (c/cascading-delete-ids! ds :film-category [fid cid])))

;; TODO: more thorough
(deftest test-subqueries
  (let [dm (c/make-data-model
             {:user {:fields [:id :username]}
              :addy {:fields [:id :user-id :street :city :state :zip]}})
        ;; NYC addresses with two occupants
        two-occupant-ny {:from :addy
                         :where [:= "New York" :city]
                         :group-by [:street :city :zip]
                         :having [:= 2 :%count.user-id]}
        ;; Users different from each other
        userq {:from :user
               :join [[:user :u2] [:< :id :u2.id]]}
        ;; Put it all together
        finalq (c/build
                 userq
                 {:select [:* :u2.* :a1.* :a2.*]
                  :join [[:addy :a3] [:= :id :a3.user-id]
                         [:addy :a4] [:= :u2.id :a4.user-id]
                         [two-occupant-ny :occ2] [:and
                                                  [:= :occ2.street :a3.street]
                                                  [:= :occ2.city :a3.city]
                                                  [:= :occ2.state :a3.state]
                                                  [:= :occ2.zip :a3.zip]
                                                  [:= :occ2.street :a4.street]
                                                  [:= :occ2.city :a4.city]
                                                  [:= :occ2.state :a4.state]
                                                  [:= :occ2.zip :a4.zip]]]
                  :left-join [[:addy :a1] [:= :id :a1.user-id]
                              [:addy :a2] [:= :u2.id :a2.user-id]]
                  :where [:not [:exists {:from :addy
                                         :select :id
                                         :where [:and
                                                 [:not= "New York" :city]
                                                 [:in :user-id [:user.id :u2.id]]]}]]})]
    (are=
      (c/to-sql dm finalq :quoting :ansi)
      ["SELECT \"user\".\"id\" AS \"id\", \"user\".\"username\" AS \"username\", \"u2\".\"id\" AS \"u2.id\", \"u2\".\"username\" AS \"u2.username\", \"a1\".\"id\" AS \"a1.id\", \"a1\".\"user_id\" AS \"a1.user_id\", \"a1\".\"street\" AS \"a1.street\", \"a1\".\"city\" AS \"a1.city\", \"a1\".\"state\" AS \"a1.state\", \"a1\".\"zip\" AS \"a1.zip\", \"a2\".\"id\" AS \"a2.id\", \"a2\".\"user_id\" AS \"a2.user_id\", \"a2\".\"street\" AS \"a2.street\", \"a2\".\"city\" AS \"a2.city\", \"a2\".\"state\" AS \"a2.state\", \"a2\".\"zip\" AS \"a2.zip\" FROM \"user\" AS \"user\" INNER JOIN \"user\" AS \"u2\" ON \"user\".\"id\" < \"u2\".\"id\" INNER JOIN \"addy\" AS \"a3\" ON \"user\".\"id\" = \"a3\".\"user_id\" INNER JOIN \"addy\" AS \"a4\" ON \"u2\".\"id\" = \"a4\".\"user_id\" INNER JOIN (SELECT \"addy\".\"id\" AS \"id\", \"addy\".\"user_id\" AS \"user_id\", \"addy\".\"street\" AS \"street\", \"addy\".\"city\" AS \"city\", \"addy\".\"state\" AS \"state\", \"addy\".\"zip\" AS \"zip\" FROM \"addy\" AS \"addy\" WHERE ? = \"addy\".\"city\" GROUP BY \"addy\".\"street\", \"addy\".\"city\", \"addy\".\"zip\" HAVING 2 = count(\"addy\".\"user_id\")) AS \"occ2\" ON (\"occ2\".\"street\" = \"a3\".\"street\" AND \"occ2\".\"city\" = \"a3\".\"city\" AND \"occ2\".\"state\" = \"a3\".\"state\" AND \"occ2\".\"zip\" = \"a3\".\"zip\" AND \"occ2\".\"street\" = \"a4\".\"street\" AND \"occ2\".\"city\" = \"a4\".\"city\" AND \"occ2\".\"state\" = \"a4\".\"state\" AND \"occ2\".\"zip\" = \"a4\".\"zip\") LEFT JOIN \"addy\" AS \"a1\" ON \"user\".\"id\" = \"a1\".\"user_id\" LEFT JOIN \"addy\" AS \"a2\" ON \"u2\".\"id\" = \"a2\".\"user_id\" WHERE NOT exists((SELECT \"user\".\"id\" AS \"id\" FROM \"addy\" AS \"addy\" WHERE (? <> \"addy\".\"city\" AND (\"addy\".\"user_id\" in (\"user\".\"id\", \"u2\".\"id\")))))" "New York" "New York"])))

(comment
  
  (run-tests)
  
  )