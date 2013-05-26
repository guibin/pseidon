(ns plugins.ds.myds)
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.ds.dummy :as d])


(r/register (d/dummy-ds "ds-test"))

(r/register (d/dummy-ds "ds-test"))


