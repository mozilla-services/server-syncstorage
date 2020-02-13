<a name="1.7.1"></a>
## 1.8.0 (2020-02-13)

#### Features

*   force specify the higher bound for basic modified ranges ([576490a0](576490a0))
*   allow disabling the BsoLastModified index ([fee296f3](fee296f3))
*   support the pool_size argument ([62a11542](62a11542), closes [#110](110))
*   add google spanner storage back-end Co-authored by Phil Jenvey <pjenvey@underboss.org> ([af652cca](af652cca))
* **auth:**  Receive FxA user-id and key-id details in Hawk token. ([3d01e614](3d01e614))
* **backend:**  Pass full user object to backend storage implementations. ([e992a39b](e992a39b))

#### Bug Fixes

*   force usage of BsoLastModified index ([52fc8d75](52fc8d75), closes [#107](107))
*   convert another IN operator usage to multiple bind params ([ba67a789](ba67a789))
* **deps:**  Update SQLAlchemy to latest version ([234e7615](234e7615))
* **docker:**  Use $DOCKER_VERSION when checking checksum. (#84); r=autrilla ([2b642b85](2b642b85))

<a name="1.7.0"></a>
## 1.7.0 (2020-02-07)


#### Features

*   improve spanner backend compatibility ([044423af](044423af), closes [#135](135))
*   Add table to cross check user migration status ([37ca921e](37ca921e))
*   Add delayed retry on 500/503 responses. ([e1c2fb4d](e1c2fb4d), closes [#121](121))
