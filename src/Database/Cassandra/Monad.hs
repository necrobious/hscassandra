{-# LANGUAGE DeriveDataTypeable, GeneralizedNewtypeDeriving #-}
module Database.Cassandra.Monad
  ( withCassandra
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
import Database.Cassandra.Types
import Network -- (PortID, PortNumber)
import Thrift.Protocol.Binary
import Thrift.Transport.Handle
import Thrift.Transport.Framed
import Database.Cassandra.Thrift.Cassandra_Types
import Database.Cassandra.Thrift.Cassandra_Client hiding (get)
import Control.Exception (bracket)
import System.IO (hClose, Handle)
import Data.Int (Int64)
import Control.Monad
import Control.Monad.State
import System.Time
import Data.Map (Map) 
import qualified Data.Map as M

withCassandra :: CassandraConfig -> Cassandra a -> IO a 
withCassandra config callback = do
  bracket
    (hOpen (cassandraHostname config, PortNumber $ fromIntegral $ cassandraPort config))
    (\ h -> tFlush h >> tClose h)
    (\ handle -> do 
       framed <- openFramedTransport handle
       let binpro = BinaryProtocol framed 
       let conn   = (binpro, binpro)
       login        conn (authreq config)
       set_keyspace conn (cassandraKeyspace config)
       liftM fst $ runCassandraT callback (config{cassandraConnection=conn})
       )
  where
  authreq CassandraConfig{cassandraUsername=u, cassandraPassword=p} = 
    AuthenticationRequest{f_AuthenticationRequest_credentials=Just $ credmap u p}
  credmap username password = M.insert "password" password (M.insert "username" username M.empty)

data CassandraConfig = CassandraConfig
  { cassandraConnection       :: (BinaryProtocol (FramedTransport Handle), BinaryProtocol (FramedTransport Handle)) 
  , cassandraKeyspace         :: String
  , cassandraConsistencyLevel :: ConsistencyLevel
  , cassandraHostname         :: Hostname
  , cassandraPort             :: Port
  , cassandraUsername         :: Username
  , cassandraPassword         :: Password
  }

instance Show CassandraConfig where
  show c = "KS: "++ (cassandraKeyspace c) ++ ", CL: " ++ (show $ cassandraConsistencyLevel c) ++ ", host: " ++ (cassandraHostname c) ++ ", port: " ++ (show $ cassandraPort c) ++ ", user: " ++ (cassandraUsername c) ++ ", pass: " ++ (cassandraPassword c) 

initConfig :: CassandraConfig
initConfig = CassandraConfig
  { cassandraConnection       = undefined
  , cassandraKeyspace         = "system"
  , cassandraConsistencyLevel = ONE
  , cassandraHostname         = "127.0.0.1"
  , cassandraPort             = 9160
  , cassandraUsername         = "default"
  , cassandraPassword         = ""
  }

getConnection :: Cassandra (BinaryProtocol (FramedTransport Handle), BinaryProtocol (FramedTransport Handle))
getConnection  = cassandraConnection `fmap` get -- CassandraT ask

getKeyspace :: Cassandra Keyspace
getKeyspace = cassandraKeyspace `fmap` get

getConsistencyLevel :: Cassandra ConsistencyLevel
getConsistencyLevel = cassandraConsistencyLevel `fmap` get

setConsistencyLevel :: ConsistencyLevel -> Cassandra ()
setConsistencyLevel consistency = 
  getCassandra >>= \ config -> put config{cassandraConsistencyLevel=consistency} 

setKeyspace :: Keyspace -> Cassandra ()
setKeyspace keyspace = do
  config <- getCassandra
  conn   <- getConnection
  liftIO $ set_keyspace conn keyspace
  put config{cassandraKeyspace=keyspace}
  
-- cassandra is VERY sensitive to its timestamp values.. as a convention, timestamps are always in microseconds.
getTime :: Cassandra Int64
getTime = do
  TOD sec pico <- liftIO getClockTime
  return $ fromInteger $ (sec * 1000000) + (toInteger $ pico `div` 1000000)

getCassandra :: Cassandra CassandraConfig
getCassandra = get 

type Cassandra a = CassandraT a
type CassandraT a = StateT CassandraConfig IO a
runCassandraT = runStateT

