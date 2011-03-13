{-# LANGUAGE TypeSynonymInstances #-}

module Database.Cassandra.Types where

import Data.ByteString.Lazy(ByteString)
import Data.ByteString.Lazy.Char8(pack, fromChunks)
import qualified Data.ByteString as Strict

type Port             = Int
type Hostname         = String
type Username         = String
type Password         = String

-- type alias' for making Cassandra's Thrift API easier to understand 
type ClusterName      = String
type Keyspace         = String
type Partitioner      = String
type Snitch           = String
type ColumnFamily     = String
type ColumnName       = ByteString 
type ColumnValue      = ByteString 
type ThriftApiVersion = String
type SchemaId         = String 


class (Ord a) => BS a where
  bs :: a -> ByteString

instance BS String where
  bs = pack 

instance BS ByteString where
  bs = id 
 
instance BS Strict.ByteString where
  bs bs' = fromChunks [bs'] 


