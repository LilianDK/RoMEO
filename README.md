
<!-- README.md is generated from README.Rmd. Please edit that file -->

# RoMEO

Retracing of Mapping Explicit to Objects (RoMEO)

## Overview

Extract relevant information from SQL scripts such as source and target
tables.

## Assumptions

  - **Target table name:** In each sql script the target table name can
    be derived from the create (table, view, procedure) statement.

## Configuration of patterns

**Unvalid patterns:**

| V1   |
| :--- |
| with |

**Valid patterns:**

| V1   |
| :--- |
| from |
| join |

**Filter patterns:**

| V1    |
| :---- |
| case  |
| where |

**Aggregation patterns:**

| V1       |
| :------- |
| GROUP BY |
| SUM      |
| AVG      |

**Create patterns:**

| V1               |
| :--------------- |
| create procedure |
| create view      |

**Insert patterns:**

| V1          |
| :---------- |
| insert into |

## Open issues

1.  **Multiple *insert into* statements**: SQL scripts can have multiple
    insert statements. This implies that the relation is \(n x m\) for
    \(n\) = target table and \(m\) = source table.
2.  **Scripts without *from* statements**: SQL scripts can just serve
    the sole purpose to calculate are prepare something and do not refer
    to a source table load.
3.  **Selected attributes**: Selected attributes shall be extracted and
    thus create per script with one target table and one source table a
    \(1 x n x m\) relation with 1 = target table, \(n\) = source
    table(s), and \(m\) = attribute(s).
4.  **Automated join loop**: Extracted SQL script shall be joined until
    end.
5.  **Re-organization of columns in output**: Columns in the output
    table shall reflect lineage steps or layer design (however in legacy
    systems layer design is often violated).
