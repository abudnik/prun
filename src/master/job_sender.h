#ifndef __JOB_SENDER_H
#define __JOB_SENDER_H

namespace master {

class JobSender
{
public:
    virtual ~JobSender() {}

    virtual void Start() = 0;

private:
};

class JobSenderBoost : public JobSender
{
public:
    virtual void Start() {}

private:
};

} // namespace master

#endif
