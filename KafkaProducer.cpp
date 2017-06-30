#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/document.h>     // rapidjson's DOM-style API
#include "KafkaProducer.h"
#include <future>
#include "dtsConfig.h"

using rapidjson::Document;
using rapidjson::Value;




namespace RTDF
{

    KafkaProducer::KafkaProducer() : bConnect_ (false)
    {
    }
    
    KafkaProducer::~KafkaProducer()
    {
    }
    
    KafkaProducer * KafkaProducer::single_ = new KafkaProducer;
    
    KafkaProducer *  KafkaProducer::instance()
    {
        return single_;
    }
    
    bool KafkaProducer::init (const dtsConfig &config)
    {
        std::string errstr;
        this->topics_.clear();
        conf_ = std::shared_ptr<RdKafka::Conf> (RdKafka::Conf::create (RdKafka::Conf::CONF_GLOBAL));
        tconf_ = std::shared_ptr<RdKafka::Conf> (RdKafka::Conf::create (RdKafka::Conf::CONF_TOPIC));
        std::async ([]() {g_dtsConf.updateHostStatus (RTDF::StateOpt::kbKafka, 0); });
        
        if (conf_.get() == nullptr || tconf_.get() == nullptr)
        {
            return false;
        }
        
        m_ipart = config.m_kafkaCfg.partitons;
        //std::string broker (host);
        // broker.append (":").append (std::to_string ( (long long) port));
        conf_->set ("metadata.broker.list", config.m_kafkaCfg.brokers, errstr);
        /* Avoid slow shutdown on error */
        {
            char hostname[128];
            gethostname (hostname, sizeof (hostname) - 1);
            conf_->set ("client.id", std::string ("rdkafka@") + hostname, errstr);
        }
        tconf_->set ("message.timeout.ms", to_string (config.m_kafkaCfg.message_timeout), errstr);
        //conf_->set ("log.thread.name", "true", errstr);
        conf_->set ("socket.timeout.ms", to_string (config.m_kafkaCfg.socket_timeout), errstr);
        //分区方案
        conf_->set ("partitioner_cb", &m_par_cb, errstr);
        conf_->set ("event_cb", &m_ex_event_cb, errstr);
        /* Set delivery report callback */
        conf_->set ("dr_cb", &m_ex_dr_cb, errstr);
        
        if (config.m_kafkaCfg.async)
        {
            conf_->set ("producer.type", "async", errstr);
            conf_->set ("queue.buffering.max.messages", std::to_string (config.m_kafkaCfg.queue_max_messages), errstr);
            conf_->set ("batch.num.messages", to_string (config.m_kafkaCfg.batch_num_messages), errstr);
            conf_->set ("queue.buffering.max.ms", to_string (config.m_kafkaCfg.queue_buffering_maxms), errstr);
        }
        
        else
        {
            conf_->set ("producer.type", "sync", errstr);
        }
        
        producer_ = std::shared_ptr<RdKafka::Producer> (RdKafka::Producer::create (conf_.get(), errstr));
        
        if (!producer_)
        {
            stringstream srr;
            srr << "无法创建kafka生产者对象 "  << errstr << std::endl;
            CLog::Instance()->LOG_WriteLine (LOG_INFO_ERROR, srr.str().c_str());
            return false;
        }
        
        bConnect_ = true;
        std::async ([]() {g_dtsConf.updateHostStatus (RTDF::StateOpt::kbKafka, 1); });
        return true;
    }
    
    int KafkaProducer::send (const char *data, const size_t &size, const std::string &topic, const int &partition, const int &timeout)
    {
        //std::unique_lock<std::mutex> locker (m_mtx);
        RdKafka::Topic *tpk = get_topic (topic);
        
        if (tpk == nullptr)
        {
            return -1;
        }
        
        if (strlen (data) < 1)
        {
            return -1;
        }
        
        Document doc;
        doc.Parse<0> (data, size);
        
        if (doc.HasParseError())
        {
            std::stringstream ss;
            rapidjson::ParseErrorCode code = doc.GetParseError();
            ss << "m_strJson文件格式出错 无法解析" << code << data << std::endl;
            CLog::Instance()->LOG_WriteLine (LOG_INFO_ERROR, ss.str().c_str());
            return -1;
        }
        
        Value & v5 = doc["sfc"];
        
        if (!v5.IsString()) {return  -1;}
        
        std::string  sfcStr = v5.GetString();
        auto rPartion = m_par_cb.partitioner_cb (tpk, &sfcStr, m_ipart, 0);
        
        while (true)
        {
            RdKafka::ErrorCode resp =
                producer_->produce (tpk, rPartion,
                                    RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                    const_cast<char*> (data), size,
                                    NULL, NULL);
                                    
            if (resp == RdKafka::ERR__QUEUE_FULL)
            {
                producer_->poll (10);
                continue;
            }
            
            if (resp != RdKafka::ERR_NO_ERROR)
            {
                stringstream srr;
                srr << "jsonProduce failed: " <<
                    RdKafka::err2str (resp) << std::endl;
                CLog::Instance()->LOG_WriteLine (LOG_INFO_ERROR, srr.str().c_str());
                
                if (bConnect_)
                {
                    std::async ([]() {g_dtsConf.updateHostStatus (RTDF::StateOpt::kbKafka, 0); });
                    bConnect_ = false;
                }
                
                return -1;
            }
            
            break;
        }
        
        if (!bConnect_)
        {
            std::async ([]() {g_dtsConf.updateHostStatus (RTDF::StateOpt::kbKafka, 1); });
            bConnect_ = true;
        }
        
        //producer_->poll (timeout); //timeout
        return size;
    }
    
    int KafkaProducer::send_batch (const std::vector<std::pair<const char *, const int &>> &data,
                                   const std::string &topic, const int &partition, const int &timeout)
    {
        std::unique_lock<std::mutex> locker (m_mtx);
        return 0;
    }
    
    bool KafkaProducer::create_topic (const std::string &topic)
    {
        std::string errstr;
        
        if (topic.empty() || producer_.get() == nullptr || tconf_.get() == nullptr)
        {
            return false;
        }
        
        if (this->get_topic (topic) != nullptr)
        {
            return false;
        }
        
        std::shared_ptr<RdKafka::Topic> tpk = std::shared_ptr<RdKafka::Topic> (RdKafka::Topic::create (producer_.get(), topic, tconf_.get(), errstr));
        
        if (!tpk)
        {
            std::cerr << "Failed to create topic: " << errstr << std::endl;
            return false;
        }
        
        topics_.insert (make_pair (topic, tpk));
        return true;
    }
    
    RdKafka::Topic *KafkaProducer::get_topic (const std::string &topic)
    {
        auto it = topics_.find (topic);
        
        if (it != topics_.end())
        {
            return it->second.get();
        }
        
        return nullptr;
    }
    
    void KafkaProducer::read_config (const char *path)
    {
    }
    
    void KafkaProducer::close()
    {
        while (producer_->outq_len() > 0)
        {
            std::cerr << "Waiting for " << producer_->outq_len() << std::endl;
            producer_->poll (1000);
        }
        
        RdKafka::wait_destroyed (1000);
    }
    
}