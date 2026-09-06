## 📚 Scenario 1

### Question

> Query to get who are getting equal salary
>
> Input :-

```text
+--------+---------+--------+------+-------------------+------+
|workerid|firstname|lastname|salary|        joiningdate|depart|
+--------+---------+--------+------+-------------------+------+
|     001|   Monika|   Arora|100000|2014-02-20 09:00:00|    HR|
|     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
|     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
|     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
|     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
+--------+---------+--------+------+-------------------+------+
```

> Excepted Output :-

```text
+--------+---------+--------+------+-------------------+------+
|workerid|firstname|lastname|salary|        joiningdate|depart|
+--------+---------+--------+------+-------------------+------+
|     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
|     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
|     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
|     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
+--------+---------+--------+------+-------------------+------+
```

### 💡 Solution

👉 [View Scenario 1 Solution](./secnerio_1.py)

---

##  Scenario 2

###  Question

> (Need the dates when the status gets changed like ordered to dispatched)
>
> Input :-

```text
+-------+----------+----------+
|orderid|statusdate|    status|
+-------+----------+----------+
|      1|     1-Jan|   Ordered|
|      1|     2-Jan|dispatched|
|      1|     3-Jan|dispatched|
|      1|     4-Jan|   Shipped|
|      1|     5-Jan|   Shipped|
|      1|     6-Jan| Delivered|
|      2|     1-Jan|   Ordered|
|      2|     2-Jan|dispatched|
|      2|     3-Jan|   shipped|
+-------+----------+----------+
```

> Excepted Output :-
>
```text
+-------+----------+----------+
|orderid|statusdate|    status|
+-------+----------+----------+
|      1|     2-Jan|dispatched|
|      1|     3-Jan|dispatched|
|      2|     2-Jan|dispatched|
+-------+----------+----------+
```

### 💡 Solution

👉 [View Scenario 2 Solution](./scenerio_2.py)
