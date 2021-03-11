# Spark Structured Streaming - GroupStateTimeout
Test a popular user sessionization use-case leveraging GroupStateTimeout with Spark structured streaming's Arbitrary Stateful stream processing

## Use sessionization use-case
POC source code to test out GroupStateTimeout with Spark structured streaming's Arbitrary Stateful stream processing for the following use-case:

* A user vists a website where he can read FAQs to help solve his problems
* User can read any number of FAQs and decide to file an issue ticket if FAQs are not helping
* If a user files an issue ticket within 5 mins of reading the last FAQ, it is considered as a `failed deflection`
* If a user files an issue ticket beyod 5 mins of reading the last FAQ, it is considered as a `successful deflection`
* If a user reads FAQs and then does no futher actions, it is considered as a `successful deflection`

## Sample Events data-set
```
Window 1
 * timestamp,user_id,faq_id/issue_id,event_type (f="faq read", i="issue filed", rest=ignore)
 * 2021-03-11 10:00:00,u1,f1,f
 * 2021-03-11 10:01:00,u1,f2,f
 * 2021-03-11 10:02:00,u1,f3,f
 * 2021-03-11 10:00:00,u2,f1,f
 * 2021-03-11 10:01:00,u2,f2,f
 * 2021-03-11 10:02:00,u2,f3,f
 * 2021-03-11 10:00:00,u3,f1,f
 * 2021-03-11 10:01:00,u3,f2,f
 * 2021-03-11 10:02:00,u3,f3,f
 * 2021-03-11 10:02:00,u6,x1,x

Window 2
* 2021-03-11 10:06:00,u1,i1,i
* 2021-03-11 10:08:00,u2,i1,i
* 2021-03-11 10:06:00,u4,f1,f
* 2021-03-11 10:07:00,u4,f2,f
* 2021-03-11 10:09:00,u4,i1,i
* 2021-03-11 10:07:00,u5,f2,f
* 2021-03-11 10:08:00,u5,f3,f
* 2021-03-11 10:07:00,u6,f1,f

Window 3
* 2021-03-11 10:14:00,u5,i1,i
* 2021-03-11 10:13:00,u1,x1,y

```

## Approach
* Use FlatMapGroupsWithState for managing user's last action state and generating successful and failed deflection events
* Use GroupStateTimeout to trigger timeouts to generate succesful deflection events if user's last action is faq read beyond 5 mins but no more further activity 
      
