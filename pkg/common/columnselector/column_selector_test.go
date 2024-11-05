package columnselector

// import (
// 	"testing"

// 	"github.com/pingcap/tiflow/pkg/config"
// 	"github.com/stretchr/testify/require"
// )

// func TestNewColumnSelector(t *testing.T) {
// 	// the column selector is not set
// 	replicaConfig := config.GetDefaultReplicaConfig()
// 	selectors, err := New(replicaConfig)
// 	require.NoError(t, err)
// 	require.NotNil(t, selectors)
// 	require.Len(t, selectors.selectors, 0)

// 	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
// 		{
// 			Matcher: []string{"test.*"},
// 			Columns: []string{"a", "b"},
// 		},
// 		{
// 			Matcher: []string{"test1.*"},
// 			Columns: []string{"*", "!a"},
// 		},
// 		{
// 			Matcher: []string{"test2.*"},
// 			Columns: []string{"co*", "!col2"},
// 		},
// 		{
// 			Matcher: []string{"test3.*"},
// 			Columns: []string{"co?1"},
// 		},
// 	}
// 	selectors, err = New(replicaConfig)
// 	require.NoError(t, err)
// 	require.Len(t, selectors.selectors, 4)
// }

// func TestColumnSelectorGetSelector(t *testing.T) {
// 	replicaConfig := config.GetDefaultReplicaConfig()
// 	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
// 		{
// 			Matcher: []string{"test.*"},
// 			Columns: []string{"a", "b"},
// 		},
// 		{
// 			Matcher: []string{"test1.*"},
// 			Columns: []string{"*", "!a"},
// 		},
// 		{
// 			Matcher: []string{"test2.*"},
// 			Columns: []string{"co*", "!col2"},
// 		},
// 		{
// 			Matcher: []string{"test3.*"},
// 			Columns: []string{"co?1"},
// 		},
// 	}
// 	selectors, err := NewColumnSelectors(replicaConfig)
// 	require.NoError(t, err)

// 	{
// 		selector := selectors.GetSelector("test", "t1")
// 		tableInfo1 := BuildTableInfo("test", "t1", []*Column{
// 			{
// 				Name: "a",
// 			},
// 			{
// 				Name: "b",
// 			},
// 			{
// 				Name: "c",
// 			},
// 		}, nil)
// 		for _, col := range tableInfo1.Columns {
// 			if col.Name.O != "c" {
// 				require.True(t, selector.Select(col))
// 			} else {
// 				require.False(t, selector.Select(col))
// 			}
// 		}
// 	}

// 	{
// 		selector := selectors.GetSelector("test1", "aaa")
// 		tableInfo1 := BuildTableInfo("test1", "aaa", []*Column{
// 			{
// 				Name: "a",
// 			},
// 			{
// 				Name: "b",
// 			},
// 			{
// 				Name: "c",
// 			},
// 		}, nil)
// 		for _, col := range tableInfo1.Columns {
// 			if col.Name.O != "a" {
// 				require.True(t, selector.Select(col))
// 			} else {
// 				require.False(t, selector.Select(col))
// 			}
// 		}
// 	}

// 	{
// 		selector := selectors.GetSelector("test2", "t2")
// 		tableInfo1 := BuildTableInfo("test2", "t2", []*Column{
// 			{
// 				Name: "a",
// 			},
// 			{
// 				Name: "col2",
// 			},
// 			{
// 				Name: "col1",
// 			},
// 		}, nil)
// 		for _, col := range tableInfo1.Columns {
// 			if col.Name.O == "col1" {
// 				require.True(t, selector.Select(col))
// 			} else {
// 				require.False(t, selector.Select(col))
// 			}
// 		}
// 	}

// 	{
// 		selector := selectors.GetSelector("test3", "t3")
// 		tableInfo1 := BuildTableInfo("test3", "t3", []*Column{
// 			{
// 				Name: "a",
// 			},
// 			{
// 				Name: "col2",
// 			},
// 			{
// 				Name: "col1",
// 			},
// 		}, nil)
// 		for _, col := range tableInfo1.Columns {
// 			if col.Name.O == "col1" {
// 				require.True(t, selector.Select(col))
// 			} else {
// 				require.False(t, selector.Select(col))
// 			}
// 		}
// 	}

// 	{
// 		selector := selectors.GetSelector("test4", "t4")
// 		tableInfo1 := BuildTableInfo("test4", "t4", []*Column{
// 			{
// 				Name: "a",
// 			},
// 			{
// 				Name: "col2",
// 			},
// 			{
// 				Name: "col1",
// 			},
// 		}, nil)
// 		for _, col := range tableInfo1.Columns {
// 			require.True(t, selector.Select(col))
// 		}
// 	}
// }
