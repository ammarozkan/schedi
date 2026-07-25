#include <schedi/list.h>

void schedi_list_init(struct schedi_list *list)
{
	list->first = NULL;
	list->last = NULL;
}

void schedi_list_add(struct schedi_list *list, struct schedi_list_node *node)
{
	node->prev = list->last;
	node->next = NULL;

	if (list->last)
		list->last->next = node;
	else
		list->first = node;

	list->last = node;
}

void schedi_list_remove(struct schedi_list *list, struct schedi_list_node *node)
{
	if (node->prev)
		node->prev->next = node->next;
	else
		list->first = node->next;

	if (node->next)
		node->next->prev = node->prev;
	else
		list->last = node->prev;
}

int schedi_list_is_empty(const struct schedi_list *list)
{
	return list->first == NULL;
}
