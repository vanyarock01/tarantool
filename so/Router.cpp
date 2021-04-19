/*
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "module.h"
#include "Router.hpp"

///////////////////////////// *** Replica *** //////////////////////////////////

Replica::Replica(Connector<Buf_t, Net_t> &connector, const std::string &name,
		 const std::string &uuid, const std::string &address,
		 size_t port, size_t weight) : m_Connector(connector),
					       m_Name(name), m_Uuid(uuid),
					       m_Address(address), m_Port(port),
					       m_Weight(weight),
					       m_Timeout(DEFAULT_NETWORK_TIMEOUT),
					       m_Master(this),
					       m_Connection(m_Connector, DECODE_AS_TUPLE)
{
}

Replica::~Replica()
{
	/*
	 * Nothing to do here. Connection is closed in the destructor of
	 * Connection class. No any other resources to release.
	 */
}

void
Replica::setMaster(Replica *master)
{
	m_Master = master;
}

bool
Replica::isMaster() const
{
	return m_Master == this;
}

int
Replica::connect()
{
	return m_Connector.connect(m_Connection, m_Address, m_Port);
}

bool
Replica::isConnected()
{
	rid_t ping = m_Connection.ping();
	if (m_Connector.wait(m_Connection, ping, m_Timeout) != 0)
		return false;
	assert(m_Connection.futureIsReady(ping));
	return true;
}

template <class T>
void
Replica::callAsync(const std::string &func, const T &args, rid_t *future)
{
	*future = m_Connection.call(func, args);
}

template <class T>
int
Replica::callSync(const std::string &func, const T &args,
		  Response<Buf_t> &result)
{
	rid_t future = m_Connection.call(func, args);
	if (m_Connector.wait(m_Connection, future, m_Timeout) != 0) {
		return box_error_set(__FILE__, __LINE__, ER_PROC_C,
				     "Failed to call function %s",
				     func.c_str());
	}
	assert(m_Connection.futureIsReady(future));
	result = *m_Connection.getResponse(future);
	return 0;
}

template <class T>
int
Replica::callSyncRaw(const std::string &func, const T &args,
		     Response<Buf_t> &result)
{
	rid_t future = m_Connection.callRaw(func, args);
	if (m_Connector.wait(m_Connection, future, m_Timeout) != 0) {
		return box_error_set(__FILE__, __LINE__, ER_PROC_C,
				     "Failed to call function %s",
				     func.c_str());
	}
	assert(m_Connection.futureIsReady(future));
	result = *m_Connection.getResponse(future);
	return 0;
}

std::string
Replica::toString()
{
	return m_Name + "(" + m_Address + ")";
}

///////////////////////////// *** ReplicaSet *** ///////////////////////////////

ReplicaSet::ReplicaSet(const std::string &uuid, size_t weight) :
	m_Uuid(uuid), m_Weight(weight), m_BucketCount(0), m_Master(nullptr)
{
}

ReplicaSet::~ReplicaSet()
{
}

void
ReplicaSet::setMaster(Replica *master)
{
	for (auto r : m_Replicas) {
		r.second->setMaster(master);
	}
	m_Master = master;
}

void
ReplicaSet::addReplica(Replica *replica)
{
	assert(replica != nullptr);
	m_Replicas.insert({replica->m_Uuid, replica});
}

int
ReplicaSet::connectAll()
{
	for (auto itr : m_Replicas) {
		Replica *r = itr.second;
		if (r->connect() != 0) {
			say_error("Failed to connect to replica %s",
				  r->toString().c_str());
			continue;
		}
		say_info("Connected replica %s", r->toString().c_str());
	}
	return 0;
}

template <class T>
int
ReplicaSet::replicaCall(const std::string &uuid,
			const std::string &func, const T &args,
			Response<Buf_t> &res)
{
	auto replica = m_Replicas.find(uuid);
	if (replica == m_Replicas.end()) {
		return box_error_set(__FILE__, __LINE__, ER_PROC_C,
				     "Can't invoke call: wrong replica uuid %s",
				     uuid.c_str());
	}
	return replica->second->callSync(func, args, res);
}

template <class T>
int
ReplicaSet::replicaCallRaw(const std::string &uuid,
			   const std::string &func, const T &args,
			   Response<Buf_t> &res)
{
	auto replica = m_Replicas.find(uuid);
	if (replica == m_Replicas.end()) {
		return box_error_set(__FILE__, __LINE__, ER_PROC_C,
				     "Can't invoke call: wrong replica uuid %s",
				     uuid.c_str());
	}
	return replica->second->callSyncRaw(func, args, res);
}

template <class T>
int
ReplicaSet::call(const std::string &func, const T &args, Response<Buf_t> &res)
{
	assert(m_Master != nullptr && "Master hasn't been assigned!");
	return m_Master->callSync(func, args, res);
}

template <class T>
int
ReplicaSet::callRaw(const std::string &func, const T &args, Response<Buf_t> &res)
{
	assert(m_Master != nullptr && "Master hasn't been assigned!");
	return m_Master->callSyncRaw(func, args, res);
}

std::string
ReplicaSet::toString()
{
	return std::string("ReplicaSet(") + m_Uuid + ")";
}

///////////////////////////// *** Router *** ///////////////////////////////////

char Router::m_TupleBuf[STATIC_TUPLE_BUF_SZ];

Router::Router(const std::string &name) : m_Name(name), m_TotalBucketCount(0),
	m_Connector(ev_default_loop(EVFLAG_AUTO))
{
}

Router::~Router()
{
	reset();
}

/**
 * GOD PLEASE FIXME
 */
int
parseURI(const char *uri, size_t uri_len, std::pair<std::string, int> &res)
{
	char delim[] = ":";
	char *uri_copy = (char *) malloc(uri_len + 1);
	if (uri_copy == NULL)
		return -1;
	memcpy(uri_copy, uri, uri_len);
	uri_copy[uri_len] = '\0';
	char *addr = strtok(uri_copy, delim);
	if (addr == NULL) {
		free(uri_copy);
		return -1;
	}
	res.first = std::string(addr);
	char *port = strtok(NULL, delim);
	if (port == NULL) {
		free(uri_copy);
		return -1;
	}
	res.second = atoi(port);
	free(uri_copy);
	return 0;
}

int
Router::addReplicaSet(struct ReplicaSetCfg *replicaSetCfg)
{
	std::vector<Replica *> replicas;
	Replica *master = nullptr;
	auto old_rs = m_ReplicaSets.find(std::string(replicaSetCfg->uuid,
						     replicaSetCfg->uuid_len));
	if (old_rs != m_ReplicaSets.end()) {
		say_warn("Replicaset with given uuid (%.*s) already exists..",
			 replicaSetCfg->uuid_len, replicaSetCfg->uuid);
		say_warn("Re-creating %s", old_rs->second->toString().c_str());
		removeReplicaSet(old_rs->second);
	}
	for (uint32_t i = 0; i < replicaSetCfg->replica_count; ++i) {
		ReplicaCfg *replicaCfg = &replicaSetCfg->replicas[i];
		std::pair<std::string, int> addr;
		if (parseURI(replicaCfg->uri, replicaCfg->uri_len, addr) != 0) {
			for (auto r : replicas) {
				delete r;
			}
			return box_error_set(__FILE__, __LINE__, ER_PROC_C,
					     "Failed to parse URI %.*s",
					     replicaCfg->uri_len,
					     replicaCfg->uri);
		}
		Replica *replica = new (std::nothrow) Replica(m_Connector,
			std::string(replicaCfg->name, replicaCfg->name_len),
			std::string(replicaCfg->uuid, replicaCfg->uuid_len),
			addr.first, addr.second, 0);
		if (replica == nullptr) {
			for (auto r : replicas) {
				delete r;
			}
			return box_error_set(__FILE__, __LINE__, ER_PROC_C,
					     "Failed to allocate memory");
		}
		if (replicaCfg->is_master) {
			master = replica;
		}
		replicas.push_back(replica);
	}
	assert(master != nullptr);
	ReplicaSet *rs = new (std::nothrow) ReplicaSet(
		std::string(replicaSetCfg->uuid, replicaSetCfg->uuid_len), 0);
	if (rs == nullptr) {
		for (auto r : replicas) {
			delete r;
		}
		return box_error_set(__FILE__, __LINE__, ER_PROC_C,
				    "Failed to allocate memory");
	}
	for (auto r : replicas) {
		if (rs->m_Replicas.find(r->m_Uuid) != rs->m_Replicas.end()) {
			say_error("Failed to add replica %s to %s");
			return box_error_set(__FILE__, __LINE__, ER_PROC_C,
					     "Failed to add replica %s to %s",
					     r->toString().c_str(),
					     rs->toString().c_str());
		}
		say_info("Added replica %s to %s", r->toString().c_str(),
			 rs->toString().c_str());
		rs->addReplica(r);
	}
	say_info("Set master %s for %s", master->toString().c_str(),
		 rs->toString().c_str());
	rs->setMaster(master);
	m_ReplicaSets.insert({rs->m_Uuid, rs});
	return 0;
}


void
Router::removeReplicaSet(ReplicaSet *rs)
{
	say_info("Removing replicaset %s", rs->toString().c_str());
	for (auto itr : rs->m_Replicas) {
		Replica *r = itr.second;
		say_info("Removing replica %s", r->toString().c_str());
		delete r;
		rs->m_Replicas.erase(itr.first);
		r = nullptr;
	}
	m_ReplicaSets.erase(rs->m_Uuid);
	delete rs;
}

void
Router::connectAll()
{
	for (auto itr : m_ReplicaSets) {
		ReplicaSet *rs = itr.second;
		(void) rs->connectAll();
		say_info("Connected replicaset %s", rs->toString().c_str());
	}
}

ReplicaSet *
Router::setBucket(size_t bucket_id, const std::string &rs_uuid)
{
	auto rs = m_ReplicaSets.find(rs_uuid);
	if (rs == m_ReplicaSets.end()) {
		/* NO_ROUTE_TO_BUCKET */
		return nullptr;
	}
	auto old_rs = m_Routes.find(bucket_id);
	if (old_rs->second != rs->second) {
		if (old_rs != m_Routes.end()) {
			old_rs->second->m_BucketCount--;
		}
		rs->second->m_BucketCount++;
	}
	m_Routes[bucket_id] = rs->second;
	return rs->second;
}

void
Router::resetBucket(size_t bucket_id)
{
	auto rs = m_Routes.find(bucket_id);
	if (rs != m_Routes.end()) {
		rs->second->m_BucketCount--;
	}
	m_Routes.erase(bucket_id);
}

template<class BUFFER>
bool
responseIsError(const Response<BUFFER> &response)
{
	return response.body.data == std::nullopt;
}

ReplicaSet *
Router::discoveryBucket(size_t bucket_id)
{
	auto rs = m_Routes.find(bucket_id);
	if (rs != m_Routes.end())
		return rs->second;
	std::tuple arg = std::make_tuple(bucket_id);
	for (auto itr : m_ReplicaSets) {
		ReplicaSet *replicaSet = itr.second;
		Response<Buf_t> response;
		if (replicaSet->call("vshard.storage.bucket_stat",
				     arg, response) != 0) {
			say_error("Failed to invoke call on %s",
				  replicaSet->toString());
			continue;
		}
		if (! responseIsError(response))
			return setBucket(bucket_id, replicaSet->m_Uuid);
	}
	box_error_set(__FILE__, __LINE__, ER_PROC_C,
		      "No route to bucket %d has been found", bucket_id);
	/* NO_ROUTE_TO_BUCKET */
	return nullptr;
}

#include "msgpuck.h"

/**
 * Hardcoded repack of msgpack: args come as decoded array of arguments.
 * We want to form following array: [bucked_id, mode, func args]
 */
static char *
repackCallArgs(uint32_t bucket_id, const char *args, const char *args_end,
	       uint32_t args_count, size_t *result_len)
{
	size_t array_tag = sizeof('\xdd') + sizeof(uint32_t);
	size_t bucked_id_tag = sizeof('\xd3') + sizeof(uint32_t);
	size_t args_size = args_end - args;
	*result_len = array_tag + bucked_id_tag + args_size;

	char *args_arr = (char *) malloc(*result_len);
	if (args_arr == NULL)
		return NULL;
	args_count = mpp::bswap(args_count);
	bucket_id = mpp::bswap(bucket_id);
	args_arr[0] = '\xdd';
	memcpy(&args_arr[1], &args_count, sizeof(uint32_t));

	args_arr[array_tag] = '\xce';
	memcpy(&args_arr[array_tag + 1], &bucket_id, sizeof(uint32_t));

	memcpy(&args_arr[array_tag + bucked_id_tag], args, args_size);
	return args_arr;
}

int
Router::call(size_t bucket_id, const char *args, const char *args_end,
	     size_t args_count, std::vector<box_tuple_t *> &results)
{
	if (bucket_id > m_TotalBucketCount) {
		box_error_set(__FILE__, __LINE__, ER_PROC_C,
			      "Bucket id %d is out of range (%d)", bucket_id,
			      m_TotalBucketCount);
		return -1;
	}
	//ReplicaSet *replicaset = discoveryBucket(bucket_id);
	ReplicaSet *replicaset = m_ReplicaSets.begin()->second;
	if (replicaset == nullptr)
		return -1;
	size_t args_len = 0;
	char *repacked_args =
		repackCallArgs(bucket_id, args, args_end, args_count, &args_len);
	if (repacked_args == NULL) {
		return box_error_set(__FILE__, __LINE__, ER_PROC_C,
				     "Failed to allocate memory!");
	}
	int rc = 0;
	Response<Buf_t> response;
	{
		/* Restrict string_view lifespan. */
		std::string_view func_args(repacked_args, args_len);
		rc = replicaset->callRaw("vshard.storage.call", func_args,
					 response);
	}
	free(repacked_args);
	if (rc != 0)
		return rc;
	if (responseIsError(response)) {
		assert(response.body.error_stack != std::nullopt);
		Error err = (*response.body.error_stack).error;
		box_error_set(err.file, err.line, err.errcode, err.msg);
		return -1;
	}
	assert(response.body.data != std::nullopt);
	std::vector<Tuple<Buf_t>> tuples = response.body.data->tuples;
	/* Result of CALL is always supplied as single array. */
	assert(tuples.size() == 1);
	Tuple<Buf_t> t = tuples[0];
	assert(response.body.data->end != t.begin);
	iterator_t<Buf_t> begin_itr = t.begin;
	iterator_t<Buf_t> end_itr = response.body.data->end;
	size_t tuple_sz = end_itr - begin_itr;
	assert(tuple_sz < STATIC_TUPLE_BUF_SZ);
	/*
	 * To create resulting tuple we have to copy content from connector's
	 * buffer to a plain buffer.
	 */
	char *tuple_buf = m_TupleBuf;
	begin_itr.get(tuple_buf, tuple_sz);
	box_tuple_format_t *fmt = box_tuple_format_default();
	box_tuple_t *tuple = box_tuple_new(fmt, tuple_buf, tuple_buf + tuple_sz);
	if (tuple == NULL) {
		box_error_set(__FILE__, __LINE__, ER_PROC_C,
			      "Failed to allocate memory!");
		for (auto to_delete : results)
			box_tuple_unref(to_delete);
		return -1;
	}
	results.push_back(tuple);
	return 0;
}

void
Router::reset()
{
	m_TotalBucketCount = 0;
	m_Routes.clear();
	for (auto itr : m_ReplicaSets)
		removeReplicaSet(itr.second);
}

void
Router::setBucketCount(uint32_t bucketCount)
{
	m_TotalBucketCount = bucketCount;
}
