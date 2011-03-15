module Database.Cassandra
  ( Column(..)
  , (=:)
  , (=|)
  , insert
  , Filter(..)
  , get
  , multiget
  , range
  , columns

  , withCassandra
  , CassandraConfig(..)
  , initConfig
  , getConnection
  , getKeyspace
  , setKeyspace
  , getConsistencyLevel
  , setConsistencyLevel
  , getTime
  , getCassandra
  , Cassandra
  , CassandraT
  ) where

import qualified Database.Cassandra.Thrift.Cassandra_Types as Thrift
import qualified Database.Cassandra.Thrift.Cassandra_Client as Cas

import Database.Cassandra.Monad
import Database.Cassandra.Types

import Data.Map (Map)
import qualified Data.Map as Map

import Data.ByteString.Lazy(ByteString)
import qualified Data.ByteString.Lazy.Char8 as Lazy

import Control.Monad.Trans
import Data.Int(Int32)


data Column = Column ColumnName ColumnValue
            | Super  ColumnName [Column] 
            deriving (Show)

-- | Build up a column's insert values
(=:) :: (BS column, BS value) => column -> value -> Column
(=:) col val = Column (bs col) (bs val)

-- | Build up a super column's insert values
(=|) :: (BS supercolumn) => supercolumn -> [Column] -> Column 
(=|) sup  = Super (bs sup)

-- insert "Users" "necrobious@gmail.com" 
--   [ "fn"      =: "Kirk"
--   , "ln"      =: "Peterson"
--   , "Address" =| [ "street1" =: "2020"
--                  , "state"   =: "Oregon"]
--   ]  
insert :: (BS key) => ColumnFamily -> key -> [Column] -> Cassandra ()
insert column_family key columns = do 
  consistency <- getConsistencyLevel
  conn <- getConnection
  now <- getTime
  liftIO $ Cas.batch_mutate conn (Map.singleton (bs key) (Map.singleton column_family (mutations now))) consistency
  where
  mutations now = map (q now) columns
  q now c = Thrift.Mutation (Just $ f now c) Nothing
  f now (Super s cs) = Thrift.ColumnOrSuperColumn Nothing (Just (Thrift.SuperColumn (Just s) (Just $ map (m now) cs))) 
  f now (Column c v) = Thrift.ColumnOrSuperColumn (Just (col c v now)) Nothing
  col c v now = Thrift.Column (Just c) (Just v) (Just now) Nothing
  m now (Column c v) = col c v now

-- remove "Users" "necrobious@gmail.com" [col "fn", "address" =| [col "state", col "city"], col "ln"]   
remove :: (BS key) => ColumnFamily -> key -> [Column]  -> Cassandra ()
remove column_family key columns = undefined

data Filter  = AllColumns
	     | ColNames [ByteString]
	     | ColRange
	         { rangeStart   :: ByteString
	         , rangeEnd     :: ByteString
	         , rangeReverse :: Bool
	         , rangeLimit   :: Int32
	         }
-- | a smarter constructor for building a Range filter
range :: (BS column_name) => column_name -> column_name -> Bool -> Int32 -> Filter
range start finish = ColRange (bs start) (bs finish) 

-- | a smarter constructor for building a Columns filter 
columns :: (BS column_name) => [column_name] -> Filter
columns = ColNames . (map bs) 


-- | for the given key, within the column family, retrieve all columns, unless filtered
get :: (BS key) => ColumnFamily -> key -> Filter -> Cassandra [Column]
get column_family key filters = do
  consistency <- getConsistencyLevel
  conn        <- getConnection
  results     <- liftIO $ Cas.get_slice conn (bs key) (column_parent column_family) (slice_predicate filters) consistency 
  return $ foldr rewrap [] results

get_count :: (BS key) => ColumnFamily -> key -> Filter -> Cassandra Int32
get_count column_family key filters = do
  consistency <- getConsistencyLevel
  conn        <- getConnection
  liftIO $ Cas.get_count conn (bs key) (column_parent column_family) (slice_predicate filters) consistency 
  

multiget :: (BS key) => ColumnFamily -> [key] -> Filter -> Cassandra (Map key [Column])
multiget column_family keys filters = do
  let byBs = keys2map keys
  consistency <- getConsistencyLevel
  conn        <- getConnection
  results     <- liftIO $ Cas.multiget_slice conn (Map.keys byBs) (column_parent column_family) (slice_predicate filters) consistency 
  return $ map2map byBs $ Map.foldrWithKey remap Map.empty results 


keys2map :: (BS key) => [key] -> Map ByteString key
keys2map keys = foldr (\ k m -> Map.insert (bs k) k m)  Map.empty keys

map2map :: (BS key) => Map ByteString key ->  Map ByteString a -> Map key a
map2map lookupMap resultsMap = Map.foldrWithKey foldOver Map.empty resultsMap 
  where
  foldOver resultKey val accMap =
    case Map.lookup resultKey lookupMap of
      Just lookupKey -> Map.insert lookupKey val accMap
      Nothing        -> accMap


remap :: (BS key) => key -> [Thrift.ColumnOrSuperColumn] -> Map key [Column] -> Map key [Column]
remap key cols acc = Map.insert key (foldr rewrap [] cols) acc 

rewrap :: Thrift.ColumnOrSuperColumn -> [Column] -> [Column]
rewrap (Thrift.ColumnOrSuperColumn (Just (Thrift.Column (Just n) (Just v) _ _)) Nothing) acc = 
  (Column n v) : acc
rewrap (Thrift.ColumnOrSuperColumn Nothing (Just (Thrift.SuperColumn (Just n) (Just cs)))) acc =
  (Super n (foldr c2c [] cs)) : acc 
rewrap _ acc = acc

c2c :: Thrift.Column -> [Column] -> [Column]
c2c (Thrift.Column  (Just n) (Just v) _ _) acc = (Column n v) : acc
c2c _ acc = acc


column_parent :: ColumnFamily -> Thrift.ColumnParent
column_parent  column_family = Thrift.ColumnParent (Just column_family) Nothing

slice_predicate :: Filter -> Thrift.SlicePredicate
slice_predicate AllColumns =
  Thrift.SlicePredicate Nothing (Just $ Thrift.SliceRange (Just Lazy.empty) (Just Lazy.empty) (Just False) (Just 100)) 
slice_predicate (ColNames bs) = 
  Thrift.SlicePredicate (Just bs) Nothing
slice_predicate (ColRange rs re rr rl) = 
  Thrift.SlicePredicate Nothing (Just (Thrift.SliceRange (Just rs) (Just re) (Just rr) (Just rl)))
  
