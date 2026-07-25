#ifndef SCHEDI_LIST_H
#define SCHEDI_LIST_H

#include <stddef.h>

/**
 * struct schedi_list_node - Node in an intrusive linked list.
 * @prev: Pointer to the previous node, or NULL if this is the first.
 * @next: Pointer to the next node, or NULL if this is the last.
 *
 * Embedded as the first field in any struct that participates in a
 * &struct schedi_list. The offset-0 guarantee lets &schedi_list_add
 * and &schedi_list_remove operate directly without container_of.
 */
struct schedi_list_node {
	struct schedi_list_node *prev, *next;
};

/**
 * struct schedi_list - Head of an intrusive linked list.
 * @first: Pointer to the first node, or NULL if the list is empty.
 * @last: Pointer to the last node, or NULL if the list is empty.
 *
 * Initialised by &schedi_list_init. Nodes are added at the tail via
 * &schedi_list_add and removed via &schedi_list_remove.
 */
struct schedi_list {
	struct schedi_list_node *first, *last;
};

/**
 * schedi_list_init - Initialise a list head to the empty state.
 * @list: The list to initialise (must not be NULL).
 *
 * After this call @list->first and @list->last are NULL.
 */
void schedi_list_init(struct schedi_list *list);

/**
 * schedi_list_add - Append a node at the end of a list.
 * @list: The list to append to.
 * @node: The node to add (must not already be in a list).
 *
 * Sets @node->prev to the old tail, @node->next to NULL, and updates
 * @list->last. If the list was empty, @list->first also points to @node.
 */
void schedi_list_add(struct schedi_list *list, struct schedi_list_node *node);

/**
 * schedi_list_remove - Unlink a node from its list.
 * @list: The list containing @node.
 * @node: The node to remove.
 *
 * Updates the neighbours' prev/next pointers and adjusts @list->first
 * or @list->last if @node was at either end. Does NOT clear @node->prev
 * or @node->next after unlinking.
 */
void schedi_list_remove(struct schedi_list *list, struct schedi_list_node *node);

/**
 * schedi_list_is_empty - Test whether a list has no nodes.
 * @list: The list to check.
 *
 * Return: 1 if the list has no nodes, 0 otherwise.
 */
int  schedi_list_is_empty(const struct schedi_list *list);

#endif /* SCHEDI_LIST_H */
