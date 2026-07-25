#include <schedi/log.h>

void LogPrint()
{
	struct schedi_flog_event events[SCHEDI_FLOG_SIZE];
	schedi_flog_get(events);

	

	for(unsigned int i = 0 ; i < SCHEDI_FLOG_SIZE ; i += 1) {
		if (events[i].tid == 0) continue;
		printf("Event from tid [%i]:[%s] with code %i\n", events[i].tid, events[i].msg,events[i].param);
	}
}