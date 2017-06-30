#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include "dtsConfig.h"
#include "producerCb.h"
#include "rdkafkacpp.h"
#include <mutex>

namespace RTDF
{

    class KafkaProducer
    {
        public:
            KafkaProducer();
            ~KafkaProducer();
            
        public:
            bool init (const dtsConfig &config);
            int  send (const char *data,
                       const size_t &size,
                       const std::string &topic,
                       const int &partition = 0,
                       const int &timeout = 0);
            int send_batch (const std::vector<std::pair<const char *, const int &>> &data,
                            const std::string &topic,
                            const int &partition = 0,
                            const int &timeout = 0);
            bool create_topic (const std::string &topic);
            void close();
            void  poll (int timeout) { producer_->poll (timeout); }
            static  KafkaProducer * instance();
            RdKafka::Topic* get_topic (const std::string &topic);
        private:
            void read_config (const char *path);
            static  KafkaProducer * single_;
            int32_t m_ipart;//分区数量
            
        private:
            std::shared_ptr<RdKafka::Producer> producer_;
            std::shared_ptr<RdKafka::Conf> conf_;
            std::shared_ptr<RdKafka::Conf> tconf_;
            std::unordered_map<std::string, std::shared_ptr<RdKafka::Topic>> topics_;
            ExampleEventCb m_ex_event_cb;
            ExampleDeliveryReportCb m_ex_dr_cb;
            MyHashPartitionerCb m_par_cb;
            std::mutex m_mtx;
            bool bConnect_;
    };
    
    typedef KafkaProducer CKPruducer;
}

