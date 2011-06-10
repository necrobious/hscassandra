module Database.Cassandra.Types where

import Data.ByteString.Lazy ( ByteString )

type Port             = Int     -- ^ Port on which Cassandra is hosted.
type Hostname         = String  -- ^ Hostname identifying Cassandra cluster.
type Username         = String  -- ^ Username for Cassandra authentication.
type Password         = String  -- ^ Password for Cassandra authentication.

type ClusterName      = String
-- | Container for Column Families. See
--   <http://wiki.apache.org/cassandra/DataModel#Keyspaces>
type Keyspace         = String
type Partitioner      = String
type Snitch           = String
-- | Container for columns (analogous to relational systems' tables).
--   See <http://wiki.apache.org/cassandra/DataModel#Column_Families>
type ColumnFamily     = String
-- | Name identifying a column, the lowest increment of data. See
--   <http://wiki.apache.org/cassandra/DataModel#Columns>
type ColumnName       = ByteString
-- | Value contained within a column. See 'ColumnName' for identifying
--   a column. See also
--   <http://wiki.apache.org/cassandra/DataModel#Columns>.
type ColumnValue      = ByteString
-- | Identifies a row within a ColumnFamily. See
--   <http://wiki.apache.org/cassandra/API#Terminology_.2BAC8_Abbreviations>.
type Key              = ByteString
type SchemaId         = String
-- | The name of a SuperColumn, a structure which stores a level of
--   Columns below the key. See
--   <http://wiki.apache.org/cassandra/API#SuperColumn>.
type SuperColumnName  = ByteString
type ThriftApiVersion = String
