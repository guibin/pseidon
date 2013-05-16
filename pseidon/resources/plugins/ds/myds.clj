(ns plugins.ds.myds)
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.ds.dummy :as d])

(r/register-ds (d/dummy-ds "test"))


