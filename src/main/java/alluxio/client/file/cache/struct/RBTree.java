package alluxio.client.file.cache.struct;

public class RBTree<T extends LinkNode> {
  private static final boolean RED = true;
  private static final boolean BLACK = false;
  public T root;

  public T get(T key)
  {
    T x = root;
    while (x != null)
    {
      int cmp = key.compareTo(key);
      if (cmp == 0) return x;
      else if (cmp < 0) x = (T)x.left;
      else if (cmp > 0) x = (T)x.right;
    }
    return null;
  }
  private void leftRotate(T x) {
    T y = (T)x.right;

    x.right = y.left;
    if (y.left != null)
      y.left.parent = x;

    y.parent = x.parent;

    if (x.parent == null) {
      this.root = y;
    } else {
      if (x.parent.left == x)
        x.parent.left = y;
      else
        x.parent.right = y;
    }
    y.left = x;
    x.parent = y;
  }

  private void rightRotate(T y) {
    T x = (T)y.left;
    y.left = x.right;
    if (x.right != null) {
			x.right.parent = y;
		}
    x.parent = y.parent;

    if (y.parent == null) {
      this.root = x;
    } else {
      if (y == y.parent.right)
        y.parent.right = x;
      else
        y.parent.left = x;
    }
    x.right = y;
    y.parent = x;
  }

  private T parentOf(T node) {
    return node!=null ? (T)node.parent : null;
  }

  private void insertFixUp(T node) {
    T parent, gparent;

    while (((parent = parentOf(node))!=null) && isRed(parent)) {
      gparent = parentOf(parent);

      if (parent == gparent.left) {
        T uncle = (T)gparent.right;
        if ((uncle!=null) && isRed(uncle)) {
          uncle.color = BLACK;
          parent.color = BLACK;
          gparent.color = BLACK;
          node = gparent;
          continue;
        }

        if (parent.right == node) {
          T tmp;
          leftRotate(parent);
          tmp = parent;
          parent = node;
          node = tmp;
        }

        parent.color = BLACK;
        gparent.color = RED;
        rightRotate(gparent);
      } else {
        T uncle = (T)gparent.left;
        if ((uncle!=null) && isRed(uncle)) {
          uncle.color = BLACK;
          parent.color = BLACK;
          gparent.color = RED;
          node = gparent;
          continue;
        }

        if (parent.left == node) {
          T tmp;
          rightRotate(parent);
          tmp = parent;
          parent = node;
          node = tmp;
        }

        parent.color = BLACK;
        gparent.color = RED;
        leftRotate(gparent);
      }
    }

    this.root.color = BLACK;
  }

  public void insert(T node) {
		if(root == null) {
  		root = node;
  		return;
		}
    int cmp;
    T y = null;
    T x = this.root;

    while (x != null) {
      y = x;
      cmp = node.compareTo(x);
      if (cmp < 0)
        x = (T)x.left;
      else
        x = (T)x.right;
    }

    node.parent = y;
    if (y!=null) {
      cmp = node.compareTo(y);
      if (cmp < 0)
        y.left = node;
      else
        y.right = node;
    } else {
      this.root = node;
    }

    node.color = RED;

    insertFixUp(node);
  }

  private void removeFixUp(T node, T parent) {
    T other;

    while ((node==null || isBlack(node)) && (node != this.root)) {
      if (parent.left!=null &&
				parent.left == node) {
        other = (T)parent.right;
        if (isRed(other)) {
          // Case 1: x的兄弟w是红色的  
          setBlack(other);
          setRed(parent);
          leftRotate(parent);
          other = (T)parent.right;
        }

        if ((other.left==null || isBlack(other.left)) &&
            (other.right==null || isBlack(other.right))) {
          // Case 2: x的兄弟w是黑色，且w的俩个孩子也都是黑色的  
          setRed(other);
          node = parent;
          parent = parentOf(node);
        } else {

          if (other.right==null || isBlack(other.right)) {
            // Case 3: x的兄弟w是黑色的，并且w的左孩子是红色，右孩子为黑色。  
            setBlack((T)other.left);
            setRed(other);
            rightRotate(other);
            other = (T)parent.right;
          }
          // Case 4: x的兄弟w是黑色的；并且w的右孩子是红色的，左孩子任意颜色。
          setColor(other,  parent.color);
          setBlack(parent);
          setBlack(other.right);
          leftRotate(parent);
          node = this.root;
          break;
        }
      } else {

        other = (T)parent.left;
        if (isRed(other)) {
          // Case 1: x的兄弟w是红色的  
          setBlack(other);
          setRed(parent);
          rightRotate(parent);
          other = (T)parent.left;
        }

        if (other!= null &&
					(other.left==null || isBlack(other.left)) &&
            (other.right==null || isBlack(other.right))) {
          // Case 2: x的兄弟w是黑色，且w的俩个孩子也都是黑色的  
          setRed(other);
          node = parent;
          parent = parentOf(node);
        } else {
					if (other != null) {
						if ((other.left == null || isBlack(other.left))) {
							// Case 3: x的兄弟w是黑色的，并且w的左孩子是红色，右孩子为黑色。
							setBlack(other.right);
							setRed(other);
							leftRotate(other);
							other = (T) parent.left;
						}
					}
					if (other != null) {
						// Case 4: x的兄弟w是黑色的；并且w的右孩子是红色的，左孩子任意颜色。
						setColor(other, parent.color);
						setBlack(parent);
						setBlack(other.left);
						rightRotate(parent);
					}
						node = this.root;
						break;
					}

      }
    }

    if (node!=null)
      setBlack(node);
  }

  void deleteNode(T node) {
    node.left = node.parent = node.right = null;
  }

   public void remove(T node) {
    T child, parent;
    boolean color;

    // 被删除节点的"左右孩子都不为空"的情况。
    if ( (node.left!=null) && (node.right!=null) ) {
      // 被删节点的后继节点。(称为"取代节点")
      // 用它来取代"被删节点"的位置，然后再将"被删节点"去掉。
      T replace = node;

      // 获取后继节点
      replace = (T)replace.right;
      while (replace.left != null)
        replace = (T)replace.left;

      // "node节点"不是根节点(只有根节点不存在父节点)
      if (parentOf(node)!=null) {
        if (parentOf(node).left == node)
          parentOf(node).left = replace;
        else
          parentOf(node).right = replace;
      } else {
        // "node节点"是根节点，更新根节点。
        this.root = replace;
      }

      // child是"取代节点"的右孩子，也是需要"调整的节点"。
      // "取代节点"肯定不存在左孩子！因为它是一个后继节点。
      child = (T)replace.right;
      parent = parentOf(replace);
      // 保存"取代节点"的颜色
      color = replace.color;

      // "被删除节点"是"它的后继节点的父节点"
      if (parent == node) {
        parent = replace;
      } else {
        // child不为空
        if (child!=null)
          setParent(child, parent);
        parent.left = child;

        replace.right = node.right;
        setParent(node.right, replace);
      }

      replace.parent = node.parent;
      replace.color = node.color;
      replace.left = node.left;
      node.left.parent = replace;

      if (color == BLACK)
        removeFixUp(child, parent);

      deleteNode(node);
      return ;
    }

    if (node.left !=null) {
      child = (T)node.left;
    } else {
      child = (T)node.right;
    }

    parent = (T)node.parent;
    // 保存"取代节点"的颜色
    color = node.color;

    if (child!=null)
      child.parent = parent;

    // "node节点"不是根节点
    if (parent!=null) {
      if (parent.left == node)
        parent.left = child;
      else
        parent.right = child;
    } else {
      this.root = child;
    }

    if (color == BLACK)
      removeFixUp(child, parent);
     deleteNode(node);
  }

  public void print(T tree, int direction) {


    if(tree != null) {

      if(direction==0)    // tree是根节点
        System.out.printf("%2s(B) is root\n", tree.toString());
      else                // tree是分支节点
        System.out.print( tree.toString() + direction);

      print((T)tree.left, -1);
      print((T)tree.right,  1);
    }
  }

  private boolean isRed(LinkNode node) {
    return ((node!=null)&&(node.color==RED)) ? true : false;
  }
  private boolean isBlack(LinkNode node) {
    return !isRed(node);
  }
  private void setBlack(LinkNode node) {
    if (node!=null)
      node.color = BLACK;
  }
  private void setRed(LinkNode node) {
    if (node!=null)
      node.color = RED;
  }
  private void setParent(LinkNode node, LinkNode parent) {
    if (node!=null)
      node.parent = parent;
  }
  private void setColor(LinkNode node, boolean color) {
    if (node!=null)
      node.color = color;
  }


}
