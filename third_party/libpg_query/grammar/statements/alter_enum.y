/*****************************************************************************
 *
 *	ALTER TYPE enumtype ADD ...
 *
 *****************************************************************************/

AlterEnumStmt:
		ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst
			{
				PGAlterEnumStmt *n = makeNode(PGAlterEnumStmt);
				n->typeName = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = NULL;
				n->newValIsAfter = true;
				n->skipIfNewValExists = $6;
				$$ = (PGNode *) n;
			}
		 | ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst BEFORE Sconst
			{
				PGAlterEnumStmt *n = makeNode(PGAlterEnumStmt);
				n->typeName = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = $9;
				n->newValIsAfter = false;
				n->skipIfNewValExists = $6;
				$$ = (PGNode *) n;
			}
		 | ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst AFTER Sconst
			{
				PGAlterEnumStmt *n = makeNode(PGAlterEnumStmt);
				n->typeName = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = $9;
				n->newValIsAfter = true;
				n->skipIfNewValExists = $6;
				$$ = (PGNode *) n;
			}
		 | ALTER TYPE_P any_name RENAME VALUE_P Sconst TO Sconst
			{
				PGAlterEnumStmt *n = makeNode(PGAlterEnumStmt);
				n->typeName = $3;
				n->oldVal = $6;
				n->newVal = $8;
				n->newValNeighbor = NULL;
				n->newValIsAfter = false;
				n->skipIfNewValExists = false;
				$$ = (PGNode *) n;
			}
		 ;

opt_if_not_exists: IF_P NOT EXISTS              { $$ = true; }
		| /* EMPTY */                          { $$ = false; }
		;