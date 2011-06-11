{-# LANGUAGE  GeneralizedNewtypeDeriving, FunctionalDependencies
            , MultiParamTypeClasses #-}

module Database.Cassandra.Monad
  ( withCassandra
  , Cassandra
  , CassandraT
  , unCassandra
  , runCassandraT
  , CassandraConfig(..)
  , initConfig
  , getConnection
  , getKeyspace
  , setKeyspace
  , getConsistencyLevel
  , setConsistencyLevel
  , getTime
  , getCassandra
  , ConsistencyLevel(..)
  , ProtoHandle
  ) where

import Control.Exception        ( bracket )
import Control.Monad            ( liftM )
import Control.Monad.State      ( MonadIO, MonadPlus, MonadState, MonadTrans
                                , StateT, get, liftIO, runStateT, put
                                )
import Data.Int                 ( Int64 )
import Data.List                ( intercalate )
import Network                  ( PortID(PortNumber) )
import System.IO                ( Handle )
import System.Time              ( ClockTime(TOD), getClockTime )
import Thrift.Protocol.Binary   ( BinaryProtocol(..) )
import Thrift.Transport.Handle  ( hOpen )
import Thrift.Transport.Framed  ( FramedTransport, openFramedTransport, tClose
                                , tFlush
                                )

import Database.Cassandra.Types ( Hostname, Keyspace, Password, Port, Username )
import Database.Cassandra.Thrift.Cassandra_Client ( login, set_keyspace )
import Database.Cassandra.Thrift.Cassandra_Types  ( AuthenticationRequest(..)
                                                  , ConsistencyLevel(..)
                                                  )

import qualified Data.Map as M

-- | A binary protocol where the handle is wraped in a framed mode.
type ProtoHandle = BinaryProtocol (FramedTransport Handle)

-- | Configuration for the Cassandra environment. Operations will be executed
--   accordingly based on these values.
data CassandraConfig = CassandraConfig
    { cassandraConnection       ::  (ProtoHandle, ProtoHandle)
    , cassandraKeyspace         ::  String
    , cassandraConsistencyLevel ::  ConsistencyLevel
    , cassandraHostname         ::  Hostname
    , cassandraPort             ::  Port
    , cassandraUsername         ::  Username
    , cassandraPassword         ::  Password
    }

newtype Cassandra a = Cassandra
    { unCassandra :: CassandraT IO a
    } deriving (Functor, Monad, MonadIO, MonadPlus, MonadState CassandraConfig)

newtype CassandraT m a = CassandraT
    { unCassandraT :: StateT CassandraConfig m a
    } deriving  ( Functor, Monad, MonadIO, MonadPlus, MonadTrans
                , MonadState CassandraConfig
                )

runCassandraT :: CassandraT m a -> CassandraConfig -> m (a, CassandraConfig)
runCassandraT  = runStateT . unCassandraT

-- | Default configuration for the Cassandra environment. Values can be changed
--   as necessary.
initConfig :: CassandraConfig
initConfig  = CassandraConfig
    { cassandraConnection       = undefined
    , cassandraKeyspace         = "system"
    , cassandraConsistencyLevel = ONE
    , cassandraHostname         = "127.0.0.1"
    , cassandraPort             = 9160
    , cassandraUsername         = "default"
    , cassandraPassword         = ""
    }

withCassandra :: CassandraConfig -> Cassandra a -> IO a
withCassandra config callback = bracket
    (hOpen (host, port))
    flushHandle $
    \handle -> do
       framed <- openFramedTransport handle
       let binpro = BinaryProtocol framed
       let conn   = (binpro, binpro)
       login        conn (authreq config)
       set_keyspace conn (cassandraKeyspace config)
       let cfg    = config { cassandraConnection = conn }
       fst `liftM` (runCassandraT . unCassandra) callback cfg
    where
        host          = cassandraHostname config
        port          = PortNumber . fromIntegral . cassandraPort $ config
        flushHandle h = tFlush h >> tClose h
        credmap u p   = M.insert "password" p . M.insert "username" u $ M.empty
        creds cfg     = credmap (cassandraUsername cfg) (cassandraPassword cfg)
        authreq cfg   = AuthenticationRequest
            { f_AuthenticationRequest_credentials = Just . creds $ cfg }

-- | Get the current Cassandra connection.
getConnection :: Cassandra (ProtoHandle, ProtoHandle)
getConnection  = cassandraConnection `fmap` get

-- | Get the 'ConsistencyLevel' being used in Cassandra operations.
getConsistencyLevel :: Cassandra ConsistencyLevel
getConsistencyLevel = cassandraConsistencyLevel `fmap` get

-- | Set the 'ConsistencyLevel' for Cassandra operations.
setConsistencyLevel :: ConsistencyLevel -> Cassandra ()
setConsistencyLevel consistency =  getCassandra >>=
    \config -> put config { cassandraConsistencyLevel = consistency }

-- | Get the keyspace in which Cassandra operations are being executed.
getKeyspace :: Cassandra Keyspace
getKeyspace = cassandraKeyspace `fmap` get

-- | Set the keyspace in which Cassandra operations are executed.
setKeyspace :: Keyspace -> Cassandra ()
setKeyspace keyspace = do
  config <- getCassandra
  conn   <- getConnection
  liftIO $ set_keyspace conn keyspace
  put config{cassandraKeyspace=keyspace}

-- | Cassandra is very sensitive with respect to the timestamp values. As a
--   convention, timestamps are always in microseconds.
getTime :: Cassandra Int64
getTime = do
  TOD sec pico <- liftIO getClockTime
  return . fromInteger $ (sec * 1000000) + (toInteger $ pico `div` 1000000)

getCassandra :: Cassandra CassandraConfig
getCassandra = get

instance Show CassandraConfig where
    show c = intercalate ", "
        [ "KS:"     ++ cassandraKeyspace c
        , "CL:"     ++ (show $ cassandraConsistencyLevel c)
        , "host:"   ++ cassandraHostname c
        , "port:"   ++ (show $ cassandraPort c)
        , "user:"   ++ cassandraUsername c
        , "pass:"   ++ cassandraPassword c
        ]
