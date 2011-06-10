{-# LANGUAGE TypeSynonymInstances #-}

module Database.Cassandra.Types where

import Data.ByteString.Lazy         ( ByteString )
import Data.ByteString.Lazy.Char8   ( pack, fromChunks )
import qualified Data.ByteString as BS

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
type ThriftApiVersion = String
type SchemaId         = String

class (Ord a) => BS a where
  bs :: a -> ByteString

instance BS String where
  bs = pack

instance BS ByteString where
  bs = id

instance BS BS.ByteString where
  bs bs' = fromChunks [bs']
