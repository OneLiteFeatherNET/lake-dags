# Changelog

## [0.2.0](https://github.com/OneLiteFeatherNET/lake-dags/compare/v0.1.0...v0.2.0) (2026-11-28)


### Features

* **dags:** add label, feature, erasure and reference statements ([#33](https://github.com/OneLiteFeatherNET/lake-dags/issues/33)) ([fb52385](https://github.com/OneLiteFeatherNET/lake-dags/commit/fb52385813071734ec38510746ba2bd4508be5a7))
* **dags:** add layer config, quality checks and packet decoder ([#31](https://github.com/OneLiteFeatherNET/lake-dags/issues/31)) ([579be1f](https://github.com/OneLiteFeatherNET/lake-dags/commit/579be1fa88eb6eca9621df3c7d3ceec37f6330b6))
* **dags:** add refined layer statements and player split ([#32](https://github.com/OneLiteFeatherNET/lake-dags/issues/32)) ([23a86b9](https://github.com/OneLiteFeatherNET/lake-dags/commit/23a86b92b6d797c110842a40169226d9f4f4eaa5))
* **dags:** declare lake tables as airflow assets ([#34](https://github.com/OneLiteFeatherNET/lake-dags/issues/34)) ([1415abb](https://github.com/OneLiteFeatherNET/lake-dags/commit/1415abbcbdfbdc8496e512f782abe9c6650ed1cc))


### Bug Fixes

* **dags:** maintain all corvus capture tables ([#26](https://github.com/OneLiteFeatherNET/lake-dags/issues/26)) ([a6a6e75](https://github.com/OneLiteFeatherNET/lake-dags/commit/a6a6e75d37d38e30ed9b9e886682e8b8474d340e))
* **dags:** run iceberg maintenance one table at a time ([#28](https://github.com/OneLiteFeatherNET/lake-dags/issues/28)) ([524dc33](https://github.com/OneLiteFeatherNET/lake-dags/commit/524dc3398e90c098452705ac45ba25d26fbd83ce))
* **dags:** size maintenance spark jobs for large tables ([#37](https://github.com/OneLiteFeatherNET/lake-dags/issues/37)) ([7d2135f](https://github.com/OneLiteFeatherNET/lake-dags/commit/7d2135f158003565296d543a5ceddbaabc0384e9))

## 0.1.0 (2026-11-18)


### Features

* **dags:** add nightly iceberg maintenance dag ([#2](https://github.com/OneLiteFeatherNET/lake-dags/issues/2)) ([71cccde](https://github.com/OneLiteFeatherNET/lake-dags/commit/71cccdeb26dabcee0299e9590de5ba3f7f885afc))
* **dags:** keep raw capture data permanently ([#14](https://github.com/OneLiteFeatherNET/lake-dags/issues/14)) ([5f0628d](https://github.com/OneLiteFeatherNET/lake-dags/commit/5f0628d1b782a9b3c40c25845836c2160b220e7f))


### Bug Fixes

* **dags:** put the spark ivy cache in a writable directory ([#20](https://github.com/OneLiteFeatherNET/lake-dags/issues/20)) ([a743003](https://github.com/OneLiteFeatherNET/lake-dags/commit/a743003a292ed07c61aa3999eb43a537a4e710cb))
* **dags:** set spark container image for maintenance jobs ([0233ad7](https://github.com/OneLiteFeatherNET/lake-dags/commit/0233ad700c991cd31707f238d485def193aa482e))
* **deps:** update dependency apache-airflow to v3.4.0 ([#8](https://github.com/OneLiteFeatherNET/lake-dags/issues/8)) ([aa443a0](https://github.com/OneLiteFeatherNET/lake-dags/commit/aa443a05d7d1599b291f9eb2cb10e070f011d8da))
* **deps:** update dependency apache-airflow-providers-cncf-kubernetes to v10.25.0 ([#9](https://github.com/OneLiteFeatherNET/lake-dags/issues/9)) ([d9f57e4](https://github.com/OneLiteFeatherNET/lake-dags/commit/d9f57e46e6c5a1b0de162b1e5faa7ffca3a810ae))
* **deps:** update dependency apache-airflow-providers-cncf-kubernetes to v11 ([#10](https://github.com/OneLiteFeatherNET/lake-dags/issues/10)) ([4593b42](https://github.com/OneLiteFeatherNET/lake-dags/commit/4593b426eca92be3c9f02e3f4f282cb6345100b0))
