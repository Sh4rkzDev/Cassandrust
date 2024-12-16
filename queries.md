# Queries

This document contains a series of queries that can be used to interact with the database. The queries are divided into the following sections:

- [Create Tables](#create-tables)
- [Insert Data](#insert-data)
- [Select Data](#select-data)
- [Update Data](#update-data)
- [Delete Data](#delete-data)
- [General Queries](#general-queries)
- [Drop Tables](#drop-tables)

## Create Tables

```sql
CREATE TABLE users (id int,name text, email text, signup_date timestamp, active boolean, PRIMARY KEY (id, name));
```

```sql
CREATE TABLE flights (flight_id int, airline text, velocity int, date timestamp, duration int, PRIMARY KEY (flight_id, date));
```

```sql
CREATE TABLE bookings (booking_id int, user_id int, flight_id int, booking_date timestamp, confirmed boolean, PRIMARY KEY (booking_id, booking_date));
```

## Insert Data

```sql
INSERT INTO users (id, name, email, signup_date, active) VALUES (101, 'Alice', 'alice@example.com', '2024-12-01T15:00:00+00:00', true);
```

```sql
INSERT INTO users (id, name, email, signup_date, active) VALUES (102, 'Bob', 'bob@example.com', '2024-12-02T12:30:00+00:00', false);
```

```sql
INSERT INTO flights (flight_id, airline, departure, arrival, date, duration) VALUES (201, 'AirwaysX', 'New York', 'London', '2024-12-19T20:00:00+00:00', 420);
```

```sql
INSERT INTO flights (flight_id, airline, departure, arrival, date, duration) VALUES (202, 'JetSet', 'Paris', 'Tokyo', '2024-12-20T09:15:00+00:00', 870);
```

```sql
INSERT INTO bookings (booking_id, user_id, flight_id, booking_date, confirmed) VALUES (301, 101, 201, '2024-12-10T14:00:00+00:00', true);
```

```sql
INSERT INTO bookings (booking_id, user_id, flight_id, booking_date, confirmed) VALUES (302, 102, 202, '2024-12-15T18:45:00+00:00', false);
```

## Select Data

```sql
SELECT id, name, email, signup_date, active FROM users WHERE id = 101 AND name = 'Alice';
```

```sql
SELECT flight_id, airline, departure, arrival, date, duration FROM flights WHERE flight_id = 201 AND date = '2024-12-19T20:00:00+00:00';
```

```sql
SELECT booking_id, user_id, flight_id, booking_date, confirmed FROM bookings WHERE booking_id = 301 AND booking_date = '2024-12-10T14:00:00+00:00';
```

## Update Data

```sql
UPDATE users SET active = false WHERE id = 101 AND name = 'Alice';
```

```sql
UPDATE flights SET duration = 430 WHERE flight_id = 201 AND date = '2024-12-19T20:00:00+00:00';
```

```sql
UPDATE bookings SET confirmed = true WHERE booking_id = 302 AND booking_date = '2024-12-15T18:45:00+00:00';
```

## Delete Data

```sql
DELETE FROM users WHERE id = 102 AND name = 'Bob';
```

```sql
DELETE FROM flights WHERE flight_id = 202 AND date = '2024-12-20T09:15:00+00:00';
```

```sql
DELETE FROM bookings WHERE booking_id = 301 AND booking_date = '2024-12-10T14:00:00+00:00';
```

## General Queries

```sql
SELECT id, name, email, signup_date, active FROM users WHERE active = true;
```

```sql
SELECT flight_id, airline, departure, arrival, date, duration FROM flights WHERE departure = 'New York' AND date >= '2024-12-01T00:00:00+00:00';
```

```sql
SELECT booking_id, user_id, flight_id, booking_date, confirmed FROM bookings WHERE confirmed = false AND booking_date < '2024-12-20T00:00:00+00:00';
```

## Drop Tables

```sql
DROP TABLE users;
```

```sql
DROP TABLE flights;
```

```sql
DROP TABLE bookings;
```
