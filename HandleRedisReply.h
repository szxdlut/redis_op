/*
 * =====================================================================================
 * 
 *       Filename:  HandleRedisReply.h
 * 
 *    Description:  处理redis回复的类
 * 
 *        Version:  1.0
 *        Created:  08/11/2014 02:41:33 PM CST
 *       Revision:  none
 *       Compiler:  gcc
 * 
 *         Author:  szxdlutsun 
 *        Company:  
 * 
 * =====================================================================================
 */

#ifndef  HANDLEREDISREPLY_INC
#define  HANDLEREDISREPLY_INC

#include "util/tc_ex.h"
#include "util/tc_common.h"
#include <hiredis.h>
#include <vector>

using namespace taf;
using namespace std;

namespace REDIS
{
    /**
     * @brief 异常类    
     */
    struct TC_Redis_Exception : public TC_Exception
    {
        TC_Redis_Exception(const string &buffer) : TC_Exception(buffer){};                                                         
        TC_Redis_Exception(const string &buffer, int err) : TC_Exception(buffer, err){};
        ~TC_Redis_Exception() throw(){};  

    };


    /* -----------------------------------------------------*/
    /**
     * 描 述: 连接信息
     */
    /* -----------------------------------------------------*/
    typedef struct 
    {
        redisContext *Context;
        string strHost;
        uint16_t usPort;
        struct timeval ConnTimeOut;         //连接超时时间
        struct timeval RWTimeOut;           //读写超时时间；如果用到了阻塞操作，这里必须全置零，否则不能达到阻塞的效果
    }SConnInfo;


    class redisConnection
    {
    public:
        redisConnection();
        ~redisConnection();

        /* -----------------------------------------------------*/
        /**
         * 描 述: CreateConn 创建一个redis连接
         *
         * 参 数: c
         * 参 数: strHost
         * 参 数: usPort
         * 参 数: stConnTimeOut
         * 参 数: stRWTimeOut
         *
         * 返回值: 
         */
        /* -----------------------------------------------------*/
        int CreateConn(redisContext*& c, const  string& strHost, uint16_t usPort, const struct timeval&  stConnTimeOut, const struct timeval& stRWTimeOut);
        int ReConn(redisContext*& c);
        int DisConn(redisContext*& c);

    private:
        int ConnToRedis(redisContext*& c, const  string& strHost, uint16_t usPort, const struct timeval&  stConnTimeOut, const struct timeval&  stRWTimeOut);
        int SetTimeOut(redisContext*& c, const struct timeval&  stRWTimeOut);

    private:
        vector<SConnInfo> m_vConnArray;
    };


    /* -----------------------------------------------------*/
    /**
     * 描 述: 建议一个线程一个全局对象，处理该线程的所有恢复
     *        由于每个回复都要删除对象指针防止泄漏，这里采用
     *        上一次请求的返回数据在下一次请求时释放。
     *        所有操作只要出现错误，当前上下文就要重新建立
     */
    /* -----------------------------------------------------*/
    class HandleReply
    {
    public:
        HandleReply()
            :m_SingleReply(NULL)
            ,m_Context(NULL)
        {
        }

        HandleReply(const redisContext** c, const redisReply* Reply)
            :m_SingleReply((redisReply*)Reply)
            ,m_Context((redisContext**)c)
        {
        }

        HandleReply(const redisContext** c)
            :m_SingleReply(NULL)
             ,m_Context((redisContext**)c)
        {
        }

        ~HandleReply() throw();

        int  GetConnection(const string& strHost, uint16_t usPort, const struct timeval&  stConnTimeOut, const struct timeval& stRWTimeOut);
        //int SetReply(const redisReply* Reply);
        /* -----------------------------------------------------*/
        /**
         * 描 述: SetContext 设置当前上下文，不会检查是否是由本对象创建的连接，但如果
         *不是本对象的连接，同时又出现了返回问题，则会抛出异常
         *
         * 参 数: c
         *
         * 返回值: 
         */
        /* -----------------------------------------------------*/
        int SetContext(const redisContext** c);

        /* -----------------------------------------------------*/
        /**
         * 描 述: SendSingleCmd  发送单个命令到redis，执行后可以调用相应的函数取值
         *
         * 参 数: strCmd
         *
         * 返回值: 
         */
        /* -----------------------------------------------------*/
        int SendSingleCmd(const string& strCmd);
        int SendSingleCmd(const vector<string>& v);

        /* -----------------------------------------------------*/
        /**
         * 描 述: SendPipeCmd 发送批量命令到redis
         *
         * 参 数: vCmd
         *
         * 返回值: 
         */
        /* -----------------------------------------------------*/
        int SendPipeCmd(const vector<string>& vCmd);
        int SendPipeCmd(const vector< vector<string> >& v);


        /* -----------------------------------------------------*/
        /**
         * 描 述: SetExpireSec 为strkey设置超时时间，单位秒
         *
         * 参 数: strKey
         * 参 数: illSeconds    小于等于0时，不设置
         *
         * 返回值: 
         */
        /* -----------------------------------------------------*/
        int SetExpireSec(const string& strKey, int64_t illSeconds);

        /* -----------------------------------------------------*/
        /**
         *  使用下面的取值函数之前要设置正确的上下文
         *  没有返回对象参数的函数要提前设置返回对象
         *  否则结果未知。。
         */
        /* -----------------------------------------------------*/
        int GetInteger(int64_t& illRsp);
        int GetInteger(const redisReply* Reply, int64_t& illRsp);
        int GetString(string& strRsp);
        int GetString(const redisReply* Reply, string& strRsp);
        int GetString(const redisReply* Reply, string& strRsp, int64_t& illStrLen);
        int GetStatu(string& strRsp);
        int GetStatu(const redisReply* Reply, string& strRsp);
        int GetArray(vector<string>& vRsp);
        int GetArray(const redisReply* Reply, vector<string>& vRsp, int64_t& illRspSize);
        int GetScanValue(vector<string>& vRsp, int64_t& illCursor);
        int GetScanValue(const redisReply* Reply, vector<string>& vRsp, int64_t& illRspSize, int64_t& illCursor);
        int GetPipeArray(vector< vector<string> >& vRsp);

    private:
        int CheckConn(const redisReply* Reply);
        void FreeSingleReply();
        void FreePipeReply();
        inline void CheckReply(const redisReply* Reply)
        {
            if(REDIS_REPLY_ERROR == Reply->type)
            {
                throw TC_Redis_Exception(string("redis cmd occur error  info = ") + Reply->str);
            }
        }

    private:
        /* -----------------------------------------------------*/
        /**
         * 描 述: 当前处理的单个回复对象
         */
        /* -----------------------------------------------------*/
        redisReply* m_SingleReply;  

        /* -----------------------------------------------------*/
        /**
         * 描 述: 当前处理的批量返回对象
         */
        /* -----------------------------------------------------*/
        vector<redisReply*> m_PipeReply;

        /* -----------------------------------------------------*/
        /**
         * 描 述: 当前的连接上下文
         *        外部要保存一下连接，否则会对这个连接失去控制
         */
        /* -----------------------------------------------------*/
        redisContext** m_Context; 

        /* -----------------------------------------------------*/
        /**
         * 描 述: 保存所有处理回复的连接，
         *用于出问题时自动重连
         */
        /* -----------------------------------------------------*/
        redisConnection m_Conn;
    };
}

#endif   /* ----- #ifndef HANDLEREDISREPLY_INC  ----- */

