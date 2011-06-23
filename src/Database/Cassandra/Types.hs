module Database.Cassandra.Types where

import Data.Int             ( Int32 )
import Data.ByteString.Lazy ( ByteString )

type Port             = Int     -- ^ Port on which Cassandra is hosted.
type Hostname         = String  -- ^ Hostname identifying Cassandra cluster.
type Username         = String  -- ^ Username for Cassandra authentication.
type Password         = String  -- ^ Password for Cassandra authentication.

type ClusterName      = String
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
-- | Container for Column Families. See
--   <http://wiki.apache.org/cassandra/DataModel#Keyspaces>
type Keyspace         = String
type Partitioner      = String
type SchemaId         = String
type Snitch           = String
-- | The name of a SuperColumn, a structure which stores a level of
--   Columns below the key. See
--   <http://wiki.apache.org/cassandra/API#SuperColumn>.
type SuperColumnName  = ByteString
type ThriftApiVersion = String
-- | An expiration time in seconds. This is primarily used for a column
--   insertion, setting a time for the inserted or updated column to expire.
type TTLTime          = Int32
